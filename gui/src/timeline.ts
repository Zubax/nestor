// ---------------------------------------------------------------------------
// Timeline renderer — CAN messages over time (adapted from Gerasim)
// ---------------------------------------------------------------------------

import type { TimelineMessage } from "./types";
import { getCanIdColor } from "./types";

const GUTTER_W = 60;
const ROW_H = 16;
const AXIS_H = 20;
const MARKER_SIZE = 4;
const MOBILE_MARKER_SIZE = 6;
const MOBILE_HIT_RADIUS = 16;

export class Timeline {
  private canvas: HTMLCanvasElement;
  private ctx: CanvasRenderingContext2D;
  private tooltip: HTMLElement;

  private messages: TimelineMessage[] = [];
  private viewStartUs = 0;
  private viewEndUs = 10_000_000; // 10s default
  private canIds: number[] = [];
  private canIdRowIndex = new Map<number, number>();
  private selectedMessage: TimelineMessage | null = null;
  private hoveredMessage: TimelineMessage | null = null;

  // Navigation
  onMessageSelect: ((msg: TimelineMessage | null) => void) | null = null;
  private userHasScrolled = false;

  // Vertical scroll offset (in pixels)
  private scrollOffsetY = 0;
  private maxScrollY = 0;

  // Touch handling
  private isMobile = false;
  private touchStartDistance = 0;
  private touchStartViewStart = 0;
  private touchStartViewEnd = 0;
  private touchStartCenter = 0;
  private lastTapTime = 0;

  private get logicalW(): number {
    return this.canvas.width / (window.devicePixelRatio || 1);
  }
  private get logicalH(): number {
    return this.canvas.height / (window.devicePixelRatio || 1);
  }

  constructor(canvas: HTMLCanvasElement, tooltip: HTMLElement) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d")!;
    this.tooltip = tooltip;
    this.isMobile =
      window.matchMedia("(max-width: 768px)").matches ||
      "ontouchstart" in window;
    this.setupInteraction();
  }

  setMessages(messages: TimelineMessage[]): void {
    this.messages = messages;

    // Extract unique CAN IDs and sort
    const idSet = new Set<number>();
    for (const m of messages) {
      idSet.add(m.canId);
    }
    this.canIds = Array.from(idSet).sort((a, b) => a - b);
    this.rebuildCanIdIndex();

    // Auto-fit time range
    if (messages.length > 0) {
      const minTime = Math.min(...messages.map((m) => m.timeUs));
      const maxTime = Math.max(...messages.map((m) => m.timeUs));
      const range = maxTime - minTime || 1_000_000;
      const padding = range * 0.05;
      this.viewStartUs = minTime - padding;
      this.viewEndUs = maxTime + padding;
    }
  }

  addMessage(msg: TimelineMessage): void {
    this.messages.push(msg);

    // Add CAN ID if new
    if (!this.canIdRowIndex.has(msg.canId)) {
      this.canIds.push(msg.canId);
      this.canIds.sort((a, b) => a - b);
      this.rebuildCanIdIndex();
    }

    // Auto-scroll to follow new messages
    if (
      !this.userHasScrolled &&
      msg.timeUs > this.viewEndUs - (this.viewEndUs - this.viewStartUs) * 0.1
    ) {
      const range = this.viewEndUs - this.viewStartUs;
      this.viewEndUs = msg.timeUs + range * 0.2;
      this.viewStartUs = this.viewEndUs - range;
    }
  }

  clear(): void {
    this.messages = [];
    this.canIds = [];
    this.canIdRowIndex.clear();
    this.selectedMessage = null;
    this.hoveredMessage = null;
    this.viewStartUs = 0;
    this.viewEndUs = 10_000_000;
    this.userHasScrolled = false;
    this.scrollOffsetY = 0;
    this.maxScrollY = 0;
  }

  private rebuildCanIdIndex(): void {
    this.canIdRowIndex.clear();
    for (let i = 0; i < this.canIds.length; i++) {
      this.canIdRowIndex.set(this.canIds[i], i);
    }
  }

  resize(): void {
    const container = this.canvas.parentElement!;
    const dpr = window.devicePixelRatio || 1;
    const w = container.clientWidth;
    const h = container.clientHeight - 5; // Account for resize handle
    this.canvas.width = w * dpr;
    this.canvas.height = h * dpr;
    this.canvas.style.width = w + "px";
    this.canvas.style.height = h + "px";
    this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  render(): void {
    const ctx = this.ctx;
    const W = this.logicalW;
    const H = this.logicalH;
    if (W === 0 || H === 0) return;

    const dpr = window.devicePixelRatio || 1;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    // Background
    ctx.fillStyle = "#1a1a1a";
    ctx.fillRect(0, 0, W, H);

    const contentH = H - AXIS_H;
    const nRows = this.canIds.length;
    const totalContentHeight = nRows * ROW_H;

    // Calculate max scroll
    this.maxScrollY = Math.max(0, totalContentHeight - contentH);
    // Clamp scroll offset
    this.scrollOffsetY = Math.max(
      0,
      Math.min(this.scrollOffsetY, this.maxScrollY),
    );

    // Save context and clip to content area (excluding axis)
    ctx.save();
    ctx.beginPath();
    ctx.rect(0, 0, W, contentH);
    ctx.clip();

    // CAN ID labels in left gutter (with scroll offset)
    ctx.font = "bold 9px monospace";
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    for (let i = 0; i < nRows; i++) {
      const y = i * ROW_H + ROW_H / 2 - this.scrollOffsetY;
      if (y < -ROW_H || y > contentH + ROW_H) continue;
      const canId = this.canIds[i];
      ctx.fillStyle = getCanIdColor(canId);
      const idStr = "0x" + canId.toString(16).toUpperCase().padStart(3, "0");
      ctx.fillText(idStr, GUTTER_W - 4, y);
    }

    // Row grid lines (with scroll offset)
    ctx.strokeStyle = "#333";
    ctx.lineWidth = 0.5;
    for (let i = 0; i <= nRows; i++) {
      const y = i * ROW_H - this.scrollOffsetY;
      if (y < -ROW_H || y > contentH + ROW_H) continue;
      ctx.beginPath();
      ctx.moveTo(GUTTER_W, y);
      ctx.lineTo(W, y);
      ctx.stroke();
    }

    // Clip plot area for markers
    ctx.beginPath();
    ctx.rect(GUTTER_W, 0, W - GUTTER_W, contentH);
    ctx.clip();

    // Draw message markers (with scroll offset)
    for (const msg of this.messages) {
      const x = this.timeToX(msg.timeUs);
      if (x < GUTTER_W - 10 || x > W + 10) continue;

      const rowIdx = this.canIdRowIndex.get(msg.canId) ?? -1;
      if (rowIdx < 0) continue;

      const y = rowIdx * ROW_H + ROW_H / 2 - this.scrollOffsetY;
      if (y < -ROW_H || y > contentH + ROW_H) continue;

      const isSelected = this.selectedMessage === msg;
      const isHovered = this.hoveredMessage === msg;
      const color = getCanIdColor(msg.canId);

      ctx.fillStyle = color;
      ctx.globalAlpha = isSelected || isHovered ? 1.0 : 0.7;

      const baseSize = this.isMobile ? MOBILE_MARKER_SIZE : MARKER_SIZE;
      ctx.beginPath();
      if (isSelected || isHovered) {
        ctx.arc(x, y, baseSize + 1, 0, Math.PI * 2);
      } else {
        ctx.arc(x, y, baseSize, 0, Math.PI * 2);
      }
      ctx.fill();

      // Draw selection ring
      if (isSelected) {
        ctx.strokeStyle = "#fff";
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.arc(x, y, MARKER_SIZE + 3, 0, Math.PI * 2);
        ctx.stroke();
      }
    }
    ctx.globalAlpha = 1.0;

    ctx.restore();

    // Time axis
    this.drawTimeAxis(ctx, W, H, contentH);

    // Draw scroll indicator if content is scrollable
    if (this.maxScrollY > 0) {
      const scrollbarH = Math.max(
        20,
        (contentH / (contentH + this.maxScrollY)) * contentH,
      );
      const scrollbarY =
        (this.scrollOffsetY / this.maxScrollY) * (contentH - scrollbarH);
      ctx.fillStyle = "rgba(255, 255, 255, 0.2)";
      ctx.fillRect(W - 6, scrollbarY, 4, scrollbarH);
    }

    // Empty state
    if (this.messages.length === 0) {
      ctx.fillStyle = "#555";
      ctx.font = "12px monospace";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText("No CAN messages", W / 2, contentH / 2);
    }
  }

  private timeToX(timeUs: number): number {
    const range = this.viewEndUs - this.viewStartUs;
    if (range <= 0) return GUTTER_W;
    const frac = (timeUs - this.viewStartUs) / range;
    return GUTTER_W + frac * (this.logicalW - GUTTER_W);
  }

  private xToTime(x: number): number {
    const plotW = this.logicalW - GUTTER_W;
    if (plotW <= 0) return this.viewStartUs;
    const frac = (x - GUTTER_W) / plotW;
    return this.viewStartUs + frac * (this.viewEndUs - this.viewStartUs);
  }

  private drawTimeAxis(
    ctx: CanvasRenderingContext2D,
    W: number,
    H: number,
    contentH: number,
  ): void {
    ctx.fillStyle = "#222";
    ctx.fillRect(0, contentH, W, AXIS_H);
    ctx.strokeStyle = "#555";
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(0, contentH);
    ctx.lineTo(W, contentH);
    ctx.stroke();

    // Tick marks
    const rangeUs = this.viewEndUs - this.viewStartUs;
    const rangeS = rangeUs / 1_000_000;
    const intervals = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60];
    let tickS = 1;
    for (const iv of intervals) {
      if (rangeS / iv < 15) {
        tickS = iv;
        break;
      }
    }

    const startTick = Math.ceil(this.viewStartUs / (tickS * 1_000_000));
    const endTick = Math.floor(this.viewEndUs / (tickS * 1_000_000));
    const decimals = tickS < 0.01 ? 3 : tickS < 0.1 ? 2 : tickS < 1 ? 1 : 0;

    ctx.font = "9px monospace";
    ctx.fillStyle = "#fff";
    ctx.textAlign = "center";
    ctx.textBaseline = "top";

    for (let i = startTick; i <= endTick; i++) {
      const tUs = i * tickS * 1_000_000;
      const x = this.timeToX(tUs);
      if (x < GUTTER_W || x > W) continue;
      ctx.beginPath();
      ctx.moveTo(x, contentH);
      ctx.lineTo(x, contentH + 4);
      ctx.stroke();
      ctx.fillText(`${(tUs / 1_000_000).toFixed(decimals)}s`, x, contentH + 5);
    }

    // Range indicator
    const rangeMs = rangeUs / 1_000;
    const rangeLabel =
      rangeMs < 1000
        ? `${rangeMs.toFixed(0)}ms`
        : `${(rangeMs / 1000).toFixed(1)}s`;
    ctx.font = "9px monospace";
    ctx.fillStyle = "#666";
    ctx.textAlign = "right";
    ctx.textBaseline = "bottom";
    ctx.fillText(rangeLabel, W - 4, H - 2);
  }

  private setupInteraction(): void {
    const canvas = this.canvas;

    // Wheel: zoom horizontally, scroll vertically
    canvas.addEventListener(
      "wheel",
      (e) => {
        e.preventDefault();
        this.userHasScrolled = true;

        const range = this.viewEndUs - this.viewStartUs;

        // Vertical scroll (no modifier or shift)
        if (
          Math.abs(e.deltaY) > Math.abs(e.deltaX) &&
          !e.ctrlKey &&
          !e.metaKey
        ) {
          // Vertical scrolling through CAN IDs
          this.scrollOffsetY = Math.max(
            0,
            Math.min(this.maxScrollY, this.scrollOffsetY + e.deltaY),
          );
        }

        // Horizontal scroll (shift + wheel or horizontal trackpad)
        if (e.shiftKey || Math.abs(e.deltaX) > Math.abs(e.deltaY)) {
          const shift = range * 0.1 * ((e.deltaX || e.deltaY) > 0 ? 1 : -1);
          this.viewStartUs += shift;
          this.viewEndUs += shift;
        }

        // Zoom (ctrl/cmd + wheel)
        if (e.ctrlKey || e.metaKey) {
          const mouseTime = this.xToTime(e.offsetX);
          const factor = e.deltaY > 0 ? 1.15 : 1 / 1.15;
          const newRange = Math.max(
            1_000,
            Math.min(range * factor, 600_000_000),
          );
          const mouseFrac = (mouseTime - this.viewStartUs) / range;
          this.viewStartUs = mouseTime - mouseFrac * newRange;
          this.viewEndUs = this.viewStartUs + newRange;
        }
      },
      { passive: false },
    );

    // Touch: pinch-to-zoom and pan
    let touchStartY = 0;
    let touchStartScrollY = 0;

    canvas.addEventListener(
      "touchstart",
      (e) => {
        if (e.touches.length === 2) {
          e.preventDefault();
          this.userHasScrolled = true;
          const touch1 = e.touches[0];
          const touch2 = e.touches[1];
          this.touchStartDistance = Math.hypot(
            touch2.clientX - touch1.clientX,
            touch2.clientY - touch1.clientY,
          );
          this.touchStartViewStart = this.viewStartUs;
          this.touchStartViewEnd = this.viewEndUs;
          // Center point for zoom
          const rect = canvas.getBoundingClientRect();
          const centerX = (touch1.clientX + touch2.clientX) / 2 - rect.left;
          this.touchStartCenter = this.xToTime(centerX);
        } else if (e.touches.length === 1) {
          // Track for vertical scroll
          touchStartY = e.touches[0].clientY;
          touchStartScrollY = this.scrollOffsetY;

          // Double-tap to reset view
          const now = Date.now();
          if (now - this.lastTapTime < 300) {
            e.preventDefault();
            this.resetView();
          }
          this.lastTapTime = now;
        }
      },
      { passive: false },
    );

    canvas.addEventListener(
      "touchmove",
      (e) => {
        if (e.touches.length === 2) {
          e.preventDefault();
          const touch1 = e.touches[0];
          const touch2 = e.touches[1];
          const currentDistance = Math.hypot(
            touch2.clientX - touch1.clientX,
            touch2.clientY - touch1.clientY,
          );

          if (this.touchStartDistance > 0) {
            const scale = this.touchStartDistance / currentDistance;
            const originalRange =
              this.touchStartViewEnd - this.touchStartViewStart;
            const newRange = Math.max(
              1_000,
              Math.min(originalRange * scale, 600_000_000),
            );

            // Zoom around center point
            const centerFrac =
              (this.touchStartCenter - this.touchStartViewStart) /
              originalRange;
            this.viewStartUs = this.touchStartCenter - centerFrac * newRange;
            this.viewEndUs = this.viewStartUs + newRange;
          }
        } else if (e.touches.length === 1) {
          // Vertical scroll with single touch (when there's scrollable content)
          if (this.maxScrollY > 0) {
            const deltaY = touchStartY - e.touches[0].clientY;
            this.scrollOffsetY = Math.max(
              0,
              Math.min(this.maxScrollY, touchStartScrollY + deltaY),
            );
          }
        }
      },
      { passive: false },
    );

    canvas.addEventListener("touchend", () => {
      this.touchStartDistance = 0;
    });

    // Pan
    let panning = false;
    let panLastX = 0;

    canvas.addEventListener("pointerdown", (e) => {
      if (e.button === 0 || e.button === 1) {
        e.preventDefault();
        panning = true;
        panLastX = e.offsetX;
        canvas.setPointerCapture(e.pointerId);
        canvas.style.cursor = "grabbing";
      }
    });

    canvas.addEventListener("pointermove", (e) => {
      if (panning) {
        this.userHasScrolled = true;
        const dx = e.offsetX - panLastX;
        const plotW = this.logicalW - GUTTER_W;
        if (plotW > 0) {
          const range = this.viewEndUs - this.viewStartUs;
          const shift = -(dx / plotW) * range;
          this.viewStartUs += shift;
          this.viewEndUs += shift;
        }
        panLastX = e.offsetX;
      } else if (!this.isMobile) {
        // Only show hover tooltip on non-mobile
        this.handleHover(e.offsetX, e.offsetY);
      }
    });

    canvas.addEventListener("pointerup", (e) => {
      if (panning) {
        panning = false;
        canvas.releasePointerCapture(e.pointerId);
        canvas.style.cursor = "";
      }
    });

    canvas.addEventListener("pointerleave", () => {
      this.tooltip.style.display = "none";
      this.hoveredMessage = null;
    });

    canvas.addEventListener("click", (e) => {
      const hit = this.hitTest(e.offsetX, e.offsetY);
      this.selectedMessage = hit;
      this.onMessageSelect?.(hit);

      // On mobile, show tooltip on tap
      if (this.isMobile && hit) {
        this.showTooltip(e.offsetX, e.offsetY, hit);
        // Auto-hide tooltip after 3 seconds
        setTimeout(() => {
          if (this.selectedMessage === hit) {
            this.tooltip.style.display = "none";
          }
        }, 3000);
      }
    });
  }

  private resetView(): void {
    if (this.messages.length > 0) {
      const minTime = Math.min(...this.messages.map((m) => m.timeUs));
      const maxTime = Math.max(...this.messages.map((m) => m.timeUs));
      const range = maxTime - minTime || 1_000_000;
      const padding = range * 0.05;
      this.viewStartUs = minTime - padding;
      this.viewEndUs = maxTime + padding;
    } else {
      this.viewStartUs = 0;
      this.viewEndUs = 10_000_000;
    }
  }

  private handleHover(x: number, y: number): void {
    const hit = this.hitTest(x, y);
    this.hoveredMessage = hit;

    if (hit) {
      this.showTooltip(x, y, hit);
    } else {
      this.tooltip.style.display = "none";
    }
  }

  private hitTest(mx: number, my: number): TimelineMessage | null {
    const contentH = this.logicalH - AXIS_H;
    if (my > contentH || mx < GUTTER_W) return null;

    // Use larger hit radius on mobile for easier touch targeting
    const hitRadius = this.isMobile ? MOBILE_HIT_RADIUS : 8;
    let closest: { msg: TimelineMessage; dist: number } | null = null;

    for (const msg of this.messages) {
      const x = this.timeToX(msg.timeUs);
      const rowIdx = this.canIdRowIndex.get(msg.canId) ?? -1;
      if (rowIdx < 0) continue;
      const y = rowIdx * ROW_H + ROW_H / 2 - this.scrollOffsetY;

      const dx = mx - x;
      const dy = my - y;
      const dist = Math.sqrt(dx * dx + dy * dy);

      if (dist < hitRadius && (!closest || dist < closest.dist)) {
        closest = { msg, dist };
      }
    }

    return closest?.msg ?? null;
  }

  private showTooltip(mx: number, my: number, msg: TimelineMessage): void {
    const idStr = "0x" + msg.canId.toString(16).toUpperCase().padStart(3, "0");
    const lines = [
      `CAN ID: ${idStr}`,
      `Time: ${(msg.timeUs / 1_000_000).toFixed(6)}s`,
      `Seq: ${msg.seqno}`,
      `Data: ${msg.dataHex.toUpperCase()}`,
    ];
    if (msg.extended) lines.push("Extended ID");
    if (msg.rtr) lines.push("RTR");
    if (msg.error) lines.push("Error frame");

    this.tooltip.textContent = lines.join("\n");
    this.tooltip.style.display = "block";
    const rect = this.canvas.parentElement!.getBoundingClientRect();
    this.tooltip.style.left = Math.min(mx + 12, rect.width - 180) + "px";
    this.tooltip.style.top = Math.max(4, my - 40) + "px";
  }
}
