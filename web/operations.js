const { api, shortID, setMessage, withBusy } = window.LockFlow;
const form = document.querySelector("#plan-form");
const message = document.querySelector("#plan-message");
const plansBody = document.querySelector("#plans");
const progressHost = document.querySelector("#vessel-progress");

let publicationsCache = new Map();
let inFlight = false;

const STATUS_LABEL = {
  pending: "Pending",
  publishing: "Publishing",
  partial: "Partial — recovering",
  completed: "Completed",
  compensating: "Cancelling",
  compensated: "Compensated",
};

const VESSEL_LABEL = {
  pending: "Pending",
  publishing: "Writing",
  published: "Published",
  failed: "Write failed",
  compensating: "Reverting",
  compensated: "Reverted",
};

function badge(status) {
  return `<span class="badge badge-${status}">${STATUS_LABEL[status] || status}</span>`;
}

function vesselBadge(status) {
  return `<span class="badge badge-vessel badge-${status}">${VESSEL_LABEL[status] || status}</span>`;
}

async function publishPlan(planID, button) {
  await withBusy(button, async () => {
    try {
      await api(`/api/plans/${planID}/signals`, { method: "POST" });
      setMessage(message, `Publication started for plan ${shortID(planID)}.`);
      await refresh();
    } catch (error) {
      setMessage(message, error.message);
      await refresh();
    }
  });
}

async function cancelPlan(planID, button) {
  await withBusy(button, async () => {
    try {
      await api(`/api/plans/${planID}/cancel`, { method: "POST" });
      setMessage(message, `Plan ${shortID(planID)} cancelled; signals compensating.`);
    } catch (error) {
      setMessage(message, error.message);
    }
    await refresh();
  });
}

function renderPlans(plans) {
  if (!plans.length) {
    plansBody.innerHTML = '<tr><td colspan="8" class="muted">No plans</td></tr>';
    return;
  }
  plansBody.innerHTML = plans.map((plan) => {
    const publication = publicationsCache.get(plan.id);
    const progress = publication
      ? `${badge(publication.status)} <span class="muted">${publication.published}/${publication.total} vessels · gen ${publication.generation}</span>`
      : '<span class="muted">Not requested</span>';
    const published = publication && publication.status === "completed";
    const settled = publication && publication.status === "compensated";
    const publishDisabled = plan.cancelled || published || settled ? "disabled" : "";
    const cancelDisabled = plan.cancelled ? "disabled" : "";
    const planState = plan.cancelled
      ? '<span class="badge badge-cancelled">Cancelled</span>'
      : '<span class="badge badge-active">Active</span>';
    return `<tr>
      <td>${plan.chamber_id}</td>
      <td>${shortID(plan.id)}</td>
      <td>${plan.direction}</td>
      <td>${plan.vessels.length}</td>
      <td>${new Date(plan.created_at).toLocaleTimeString()}</td>
      <td>${planState}</td>
      <td>${progress}</td>
      <td class="actions">
        <button data-action="publish" data-plan="${plan.id}" ${publishDisabled}>Publish signals</button>
        <button class="danger" data-action="cancel" data-plan="${plan.id}" ${cancelDisabled}>Cancel</button>
      </td>
    </tr>`;
  }).join("");
}

function renderVesselProgress(publications) {
  const visible = publications.filter((item) =>
    ["publishing", "partial", "compensating", "completed", "compensated"].includes(item.status));
  if (!visible.length) {
    progressHost.innerHTML = '<span class="muted">No publications yet.</span>';
    return;
  }
  progressHost.innerHTML = visible.map((publication) => {
    const rows = publication.vessels.map((vessel) => `
      <div class="progress-row">
        <span class="vessel-name">${vessel.vessel_name} <span class="muted">${vessel.call_sign}</span></span>
        ${vesselBadge(vessel.status)}
        <span class="muted">attempt ${vessel.attempts}</span>
        ${vessel.error ? `<span class="progress-error">${vessel.error}</span>` : ""}
      </div>`).join("");
    const note = publication.cancel_note ? `<span class="muted"> · ${publication.cancel_note}</span>` : "";
    return `<div class="progress-block">
      <div class="progress-head">${shortID(publication.plan_id)} · ${publication.chamber_id} ${badge(publication.status)} ${note}</div>
      ${rows}
    </div>`;
  }).join("");
}

async function refresh() {
  const [plansData, chambers, health] = await Promise.all([
    api("/api/plans"), api("/api/chambers"), api("/healthz"),
  ]);
  document.querySelector("#health").textContent = health.status === "ok" ? "Devices online" : "Attention required";
  publicationsCache = new Map((plansData.publications || []).map((item) => [item.plan_id, item]));
  renderPlans(plansData.plans || []);
  renderVesselProgress(plansData.publications || []);

  const current = (chambers || []).find((item) => item.cycle);
  document.querySelector("#metric-chamber").textContent = current?.chamber_id || "Idle";
  document.querySelector("#metric-stage").textContent = current?.cycle?.stage || "-";
  document.querySelector("#metric-cycle").textContent = shortID(current?.cycle?.id);

  inFlight = (plansData.publications || []).some((item) =>
    ["pending", "publishing", "partial", "compensating"].includes(item.status));
}

plansBody.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  if (button.dataset.action === "publish") {
    publishPlan(button.dataset.plan, button);
  } else if (button.dataset.action === "cancel") {
    cancelPlan(button.dataset.plan, button);
  }
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = form.querySelector("button");
  await withBusy(button, async () => {
    try {
      const result = await api("/api/plans", { method: "POST", body: JSON.stringify({
        chamber_id: document.querySelector("#chamber").value,
        direction: document.querySelector("#direction").value,
        vessels: [{ name: document.querySelector("#vessel").value, call_sign: document.querySelector("#call-sign").value, length_m: Number(document.querySelector("#length").value) }],
      }) });
      setMessage(message, `Plan ${shortID(result.plan.id)} admitted to ${result.plan.chamber_id}.`);
      await refresh();
    } catch (error) { setMessage(message, error.message); }
  });
});

async function loop() {
  try {
    await refresh();
  } catch (error) {
    setMessage(message, error.message);
  }
  // Poll quickly while vessels are still being written or compensated.
  setTimeout(loop, inFlight ? 1000 : 5000);
}

loop();
