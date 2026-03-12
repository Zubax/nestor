// ---------------------------------------------------------------------------
// CF3D API client
// ---------------------------------------------------------------------------

import type { DevicesResponse, BootsResponse, RecordsResponse } from './types';

const BASE = '/cf3d/api/v1';

export const api = {
  async getDevices(): Promise<DevicesResponse> {
    const res = await fetch(`${BASE}/devices`);
    if (!res.ok) throw new Error(`Failed to fetch devices: ${res.status}`);
    return res.json();
  },

  async getBoots(
    device: string,
    earliest?: string,
    latest?: string
  ): Promise<BootsResponse> {
    const params = new URLSearchParams({ device });
    if (earliest) params.set('earliest_commit', earliest);
    if (latest) params.set('latest_commit', latest);
    const res = await fetch(`${BASE}/boots?${params}`);
    if (!res.ok) throw new Error(`Failed to fetch boots: ${res.status}`);
    return res.json();
  },

  async getRecords(
    device: string,
    bootIds: number[],
    opts?: {
      seqnoMin?: number;
      seqnoMax?: number;
      limit?: number;
      waitTimeout?: number;
    }
  ): Promise<RecordsResponse> {
    const params = new URLSearchParams({ device });
    bootIds.forEach(id => params.append('boot_id', String(id)));
    if (opts?.seqnoMin !== undefined) params.set('seqno_min', String(opts.seqnoMin));
    if (opts?.seqnoMax !== undefined) params.set('seqno_max', String(opts.seqnoMax));
    if (opts?.limit !== undefined) params.set('limit', String(opts.limit));
    if (opts?.waitTimeout !== undefined) params.set('wait_timeout_s', String(opts.waitTimeout));
    const res = await fetch(`${BASE}/records?${params}`);
    if (!res.ok) throw new Error(`Failed to fetch records: ${res.status}`);
    return res.json();
  }
};
