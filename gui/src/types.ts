// ---------------------------------------------------------------------------
// Type definitions for CF3D API
// ---------------------------------------------------------------------------

export interface Frame {
  can_id: number;
  extended: boolean;
  rtr: boolean;
  error: boolean;
  data_hex: string;
}

export interface Record {
  hw_ts_us: number;
  boot_id: number;
  seqno: number;
  commit_ts: number;
  frame: Frame;
}

export interface Device {
  device: string;
  last_heard_ts: number;
  last_uid: number;
}

export interface Boot {
  boot_id: number;
  first_record: Record;
  last_record: Record;
}

export interface DevicesResponse {
  devices: Device[];
}

export interface BootsResponse {
  device: string;
  boots: Boot[];
}

export interface RecordsResponse {
  device: string;
  records: Record[];
  latest_seqno_seen: number;
}

// ---------------------------------------------------------------------------
// Internal types for rendering
// ---------------------------------------------------------------------------

export interface DeviceNode {
  device: string;
  x: number;
  y: number;
  lastHeardTs: number;
  lastUid: number;
  bootCount: number;
  isOnline: boolean;
}

export interface TimelineMessage {
  id: number;
  timeUs: number;
  canId: number;
  extended: boolean;
  rtr: boolean;
  error: boolean;
  dataHex: string;
  bootId: number;
  seqno: number;
}

// Color palette for CAN IDs (based on Gerasim's scheme)
export const CAN_ID_COLORS: string[] = [
  '#f1c40f', // yellow
  '#e67e22', // orange
  '#e74c3c', // red
  '#9b59b6', // purple
  '#3498db', // blue
  '#1abc9c', // teal
  '#2ecc71', // green
  '#fd79a8', // pink
  '#00cec9', // cyan
  '#fdcb6e', // light yellow
  '#a29bfe', // lavender
  '#fab1a0', // salmon
];

export function getCanIdColor(canId: number): string {
  return CAN_ID_COLORS[canId % CAN_ID_COLORS.length];
}
