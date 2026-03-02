from dataclasses import dataclass

CAN_EFF_FLAG = 0x80000000
CAN_RTR_FLAG = 0x40000000
CAN_ERR_FLAG = 0x20000000
"""
CAN ID flags mirroring those in the Linux kernel.
"""

CAN_ID_EXT_MASK = 0x1FFFFFFF


@dataclass(frozen=True)
class CANFrameRecord:
    hw_ts_us: int
    """Time in microseconds from device bootup to frame capture."""
    boot_id: int
    """Sequential boot count; incremented each bootup."""
    seqno: int
    """Unique sequential number of the frame on this device across reboots."""
    frame: "CANFrame"


@dataclass(frozen=True)
class CANFrameRecordCommitted(CANFrameRecord):
    commit_ts: int
    """Unix timestamp when committed."""


@dataclass(frozen=True)
class CANFrame:
    can_id_with_flags: int
    data: bytes | bytearray

    @property
    def can_id(self) -> int:
        return self.can_id_with_flags & CAN_ID_EXT_MASK

    @property
    def extended(self) -> bool:
        return bool(self.can_id_with_flags & CAN_EFF_FLAG)

    @property
    def rtr(self) -> bool:
        return bool(self.can_id_with_flags & CAN_RTR_FLAG)

    @property
    def error(self) -> bool:
        return bool(self.can_id_with_flags & CAN_ERR_FLAG)
