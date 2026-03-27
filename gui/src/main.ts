// ---------------------------------------------------------------------------
// Nestor — CF3D CAN Data Viewer
// Entry point
// ---------------------------------------------------------------------------

import { api } from "./api";
import { Timeline } from "./timeline";
import type { Device, Boot, Record, TimelineMessage } from "./types";

// State
let devices: Device[] = [];
let boots: Boot[] = [];
let records: Record[] = [];
let selectedDevice: string | null = null;
let selectedBoot: number | null = null;

// Persistent cache backed by sessionStorage
const cache = {
  get<T>(key: string): T | null {
    const raw = sessionStorage.getItem(`nestor:${key}`);
    if (!raw) return null;
    try { return JSON.parse(raw) as T; } catch { return null; }
  },
  set(key: string, value: unknown): void {
    sessionStorage.setItem(`nestor:${key}`, JSON.stringify(value));
  },
};

// Components
let timeline: Timeline;

// DOM elements
let deviceSelect: HTMLSelectElement;
let bootSelect: HTMLSelectElement;
let deviceList: HTMLUListElement;
let bootList: HTMLUListElement;
let recordsBody: HTMLTableSectionElement;
let statusLeft: HTMLElement;
let statusRight: HTMLElement;
let connectionStatus: HTMLElement;
let sidePanelToggle: HTMLButtonElement;
let sidePanel: HTMLElement;

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

function init(): void {
  // Get DOM elements
  const timelineCanvas = document.getElementById(
    "timeline-canvas",
  ) as HTMLCanvasElement;
  deviceSelect = document.getElementById("device-select") as HTMLSelectElement;
  bootSelect = document.getElementById("boot-select") as HTMLSelectElement;
  deviceList = document.getElementById("device-list") as HTMLUListElement;
  bootList = document.getElementById("boot-list") as HTMLUListElement;
  recordsBody = document.getElementById(
    "records-body",
  ) as HTMLTableSectionElement;
  statusLeft = document.getElementById("status-left") as HTMLElement;
  statusRight = document.getElementById("status-right") as HTMLElement;
  connectionStatus = document.getElementById(
    "connection-status",
  ) as HTMLElement;

  const timelineTooltip = document.getElementById(
    "timeline-tooltip",
  ) as HTMLElement;
  sidePanelToggle = document.getElementById(
    "side-panel-toggle",
  ) as HTMLButtonElement;
  sidePanel = document.getElementById("side-panel") as HTMLElement;

  // Initialize components
  timeline = new Timeline(timelineCanvas, timelineTooltip);

  // Event handlers
  timeline.onMessageSelect = (msg) => {
    if (msg) {
      scrollToRecord(msg.seqno);
    }
  };

  deviceSelect.addEventListener("change", () => {
    selectDevice(deviceSelect.value || null);
  });

  bootSelect.addEventListener("change", () => {
    selectBoot(bootSelect.value ? parseInt(bootSelect.value) : null);
  });

  // Setup mobile side panel toggle
  setupMobileSidePanel();

  // Setup resize handlers
  setupResize();

  // Initial resize
  resizeTimeline();
  window.addEventListener("resize", resizeTimeline);

  // Load initial data
  loadDevices();

  // Start render loop
  requestAnimationFrame(tick);
}

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------

async function loadDevices(): Promise<void> {
  const cached = cache.get<Device[]>("devices");
  if (cached) {
    devices = cached;
    connectionStatus.textContent = `${devices.length} device(s)`;
    connectionStatus.className = "status connected";
    updateDeviceList();
    statusLeft.textContent = `Loaded ${devices.length} device(s) (cached)`;
    return;
  }
  try {
    connectionStatus.textContent = "Connecting...";
    connectionStatus.className = "status";

    const res = await api.getDevices();
    devices = res.devices || [];
    cache.set("devices", devices);

    connectionStatus.textContent = `${devices.length} device(s)`;
    connectionStatus.className = "status connected";

    updateDeviceList();

    statusLeft.textContent = `Loaded ${devices.length} device(s)`;
  } catch (err) {
    connectionStatus.textContent = "Connection error";
    connectionStatus.className = "status error";
    statusLeft.textContent = `Error: ${err instanceof Error ? err.message : "Unknown error"}`;
    console.error("Failed to load devices:", err);
  }
}

async function loadBoots(device: string): Promise<void> {
  const cached = cache.get<Boot[]>(`boots:${device}`);
  if (cached) {
    boots = cached;
    updateBootList();
    statusLeft.textContent = `${boots.length} boot session(s) (cached)`;
    return;
  }
  try {
    statusLeft.textContent = "Loading boots...";
    const res = await api.getBoots(device);
    boots = res.boots || [];
    cache.set(`boots:${device}`, boots);
    updateBootList();
    statusLeft.textContent = `${boots.length} boot session(s)`;
  } catch (err) {
    statusLeft.textContent = `Error loading boots: ${err instanceof Error ? err.message : "Unknown"}`;
    console.error("Failed to load boots:", err);
  }
}

async function loadRecords(device: string, bootId: number): Promise<void> {
  const cacheKey = `records:${device}:${bootId}`;
  const cached = cache.get<Record[]>(cacheKey);
  if (cached) {
    records = cached;
    updateRecordsTable();
    updateTimeline();
    statusLeft.textContent = `${records.length} CAN message(s) (cached)`;
    statusRight.textContent = `Boot #${bootId}`;
    return;
  }
  try {
    statusLeft.textContent = "Loading records...";
    const res = await api.getRecords(device, [bootId], { limit: 1000 });
    records = res.records || [];
    cache.set(cacheKey, records);

    updateRecordsTable();
    updateTimeline();

    statusLeft.textContent = `${records.length} CAN message(s)`;
    statusRight.textContent = `Boot #${bootId}`;
  } catch (err) {
    statusLeft.textContent = `Error loading records: ${err instanceof Error ? err.message : "Unknown"}`;
    console.error("Failed to load records:", err);
  }
}

// ---------------------------------------------------------------------------
// Selection handlers
// ---------------------------------------------------------------------------

function selectDevice(device: string | null): void {
  selectedDevice = device;
  selectedBoot = null;
  boots = [];
  records = [];

  // Update UI
  deviceSelect.value = device || "";
  bootSelect.value = "";
  bootSelect.disabled = !device;
  updateDeviceListSelection();
  updateBootList();
  updateRecordsTable();
  timeline.clear();

  if (device) {
    loadBoots(device);
  }
}

function selectBoot(bootId: number | null): void {
  selectedBoot = bootId;
  records = [];

  bootSelect.value = bootId?.toString() || "";
  updateBootListSelection();
  updateRecordsTable();
  timeline.clear();

  if (selectedDevice && bootId !== null) {
    loadRecords(selectedDevice, bootId);
    // Collapse side panel on mobile after boot selection
    collapseSidePanelOnMobile();
  }
}

// ---------------------------------------------------------------------------
// UI updates
// ---------------------------------------------------------------------------

function updateDeviceList(): void {
  deviceList.innerHTML = "";
  deviceSelect.innerHTML = '<option value="">Select device...</option>';

  for (const d of devices) {
    // List item
    const li = document.createElement("li");
    li.dataset.device = d.device;
    li.innerHTML = `
      <span class="device-name">${escapeHtml(d.device)}</span>
      <span class="device-meta">UID: ${d.last_uid.toString(16).toUpperCase()} · ${formatTimeAgo(d.last_heard_ts)}</span>
    `;
    li.addEventListener("click", () => selectDevice(d.device));
    deviceList.appendChild(li);

    // Select option
    const option = document.createElement("option");
    option.value = d.device;
    option.textContent = d.device;
    deviceSelect.appendChild(option);
  }

  if (devices.length === 0) {
    deviceList.innerHTML = '<li class="empty-state">No devices found</li>';
  }
}

function updateDeviceListSelection(): void {
  for (const li of deviceList.querySelectorAll("li")) {
    if (li.dataset.device === selectedDevice) {
      li.classList.add("selected");
    } else {
      li.classList.remove("selected");
    }
  }
}

function updateBootList(): void {
  bootList.innerHTML = "";
  bootSelect.innerHTML = '<option value="">Select boot...</option>';

  for (const b of boots) {
    // List item
    const li = document.createElement("li");
    li.dataset.bootId = b.boot_id.toString();
    const startTime = new Date(
      b.first_record.commit_ts * 1000,
    ).toLocaleString();
    const endTime = new Date(b.last_record.commit_ts * 1000).toLocaleString();
    li.innerHTML = `
      <span class="boot-id">Boot #${b.boot_id}</span>
      <span class="boot-meta">${startTime}</span>
      <span class="boot-meta">&rarr; ${endTime}</span>
    `;
    li.addEventListener("click", () => selectBoot(b.boot_id));
    bootList.appendChild(li);

    // Select option
    const option = document.createElement("option");
    option.value = b.boot_id.toString();
    option.textContent = `Boot #${b.boot_id}`;
    bootSelect.appendChild(option);
  }

  if (boots.length === 0 && selectedDevice) {
    bootList.innerHTML = '<li class="empty-state">No boot sessions</li>';
  } else if (!selectedDevice) {
    bootList.innerHTML = '<li class="empty-state">Select a device</li>';
  }
}

function updateBootListSelection(): void {
  for (const li of bootList.querySelectorAll("li")) {
    if (li.dataset.bootId === selectedBoot?.toString()) {
      li.classList.add("selected");
    } else {
      li.classList.remove("selected");
    }
  }
}

function updateRecordsTable(): void {
  recordsBody.innerHTML = "";

  if (records.length === 0) {
    recordsBody.innerHTML = `
      <tr>
        <td colspan="6" class="empty-state">
          ${selectedBoot === null ? "Select a boot session to view records" : "No records found"}
        </td>
      </tr>
    `;
    return;
  }

  const html: string[] = [];
  for (const r of records) {
    const canIdStr =
      "0x" + r.frame.can_id.toString(16).toUpperCase().padStart(3, "0");
    html.push(
      `<tr data-seqno="${r.seqno}">`,
      `<td class="mono timestamp">${r.hw_ts_us.toLocaleString()}</td>`,
      `<td class="mono">${r.seqno}</td>`,
      `<td class="mono can-id">${canIdStr}</td>`,
      `<td>${r.frame.extended ? "✓" : ""}</td>`,
      `<td>${r.frame.rtr ? "✓" : ""}</td>`,
      `<td class="mono can-data">${r.frame.data_hex.toUpperCase()}</td>`,
      `</tr>`,
    );
  }
  recordsBody.innerHTML = html.join("");
}

function updateTimeline(): void {
  const messages: TimelineMessage[] = records.map((r, i) => ({
    id: i,
    timeUs: r.hw_ts_us,
    canId: r.frame.can_id,
    extended: r.frame.extended,
    rtr: r.frame.rtr,
    error: r.frame.error,
    dataHex: r.frame.data_hex,
    bootId: r.boot_id,
    seqno: r.seqno,
  }));
  timeline.setMessages(messages);
}

function scrollToRecord(seqno: number): void {
  const row = recordsBody.querySelector(`tr[data-seqno="${seqno}"]`);
  if (row) {
    row.scrollIntoView({ behavior: "smooth", block: "center" });
    row.classList.add("selected");
    setTimeout(() => row.classList.remove("selected"), 2000);
  }
}

// ---------------------------------------------------------------------------
// Resize handling
// ---------------------------------------------------------------------------

function resizeTimeline(): void {
  timeline.resize();
}

function setupResize(): void {
  const timelineResize = document.getElementById("timeline-resize")!;
  const timelineContainer = document.getElementById("timeline-container")!;
  setupVerticalResize(timelineResize, timelineContainer, 80, 500);
}

function setupVerticalResize(
  handle: HTMLElement,
  container: HTMLElement,
  minH: number,
  maxH: number,
): void {
  let dragging = false;
  let startY = 0;
  let startH = 0;

  handle.addEventListener("pointerdown", (e) => {
    e.preventDefault();
    dragging = true;
    startY = e.clientY;
    startH = container.offsetHeight;
    handle.setPointerCapture(e.pointerId);
    document.body.style.cursor = "ns-resize";
  });

  handle.addEventListener("pointermove", (e) => {
    if (!dragging) return;
    const newH = Math.max(minH, Math.min(maxH, startH + (e.clientY - startY)));
    container.style.height = newH + "px";
    resizeTimeline();
  });

  handle.addEventListener("pointerup", (e) => {
    if (!dragging) return;
    dragging = false;
    handle.releasePointerCapture(e.pointerId);
    document.body.style.cursor = "";
  });
}

// ---------------------------------------------------------------------------
// Render loop
// ---------------------------------------------------------------------------

function tick(): void {
  timeline.render();
  requestAnimationFrame(tick);
}

// ---------------------------------------------------------------------------
// Mobile support
// ---------------------------------------------------------------------------

function setupMobileSidePanel(): void {
  sidePanelToggle.addEventListener("click", () => {
    sidePanel.classList.toggle("collapsed");
    const isCollapsed = sidePanel.classList.contains("collapsed");
    sidePanelToggle.textContent = isCollapsed
      ? "☰ Show Devices & Boots"
      : "☰ Hide Devices & Boots";
  });

  // Auto-collapse side panel on mobile after selection
  if (window.matchMedia("(max-width: 768px)").matches) {
    sidePanel.classList.add("collapsed");
    sidePanelToggle.textContent = "☰ Show Devices & Boots";
  }
}

function collapseSidePanelOnMobile(): void {
  if (window.matchMedia("(max-width: 768px)").matches) {
    sidePanel.classList.add("collapsed");
    sidePanelToggle.textContent = "☰ Show Devices & Boots";
  }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

function escapeHtml(s: string): string {
  return s.replace(
    /[&<>"']/g,
    (c) =>
      ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      })[c] || c,
  );
}

function formatTimeAgo(ts: number): string {
  const now = Date.now() / 1000;
  const diff = now - ts;
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

document.addEventListener("DOMContentLoaded", init);
