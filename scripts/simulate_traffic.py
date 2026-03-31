#!/usr/bin/env python3
"""Simulate DroneCAN traffic on a virtual CAN interface for GUI plotting."""

import argparse
import math
import time
import dronecan


def main():
    parser = argparse.ArgumentParser(description='Simulate DroneCAN traffic for plotting')
    parser.add_argument('--iface', default='vcan0', help='CAN interface (default: vcan0)')
    parser.add_argument('--node-id', type=int, default=10, help='Node ID (default: 10)')
    parser.add_argument('--rate', type=float, default=10.0, help='Message rate in Hz (default: 10)')
    args = parser.parse_args()

    node = dronecan.make_node(args.iface, node_id=args.node_id)
    period = 1.0 / args.rate

    print(f'Publishing on {args.iface} as node {args.node_id} at {args.rate} Hz')
    print('Press Ctrl+C to stop')

    try:
        while True:
            t = time.time()

            # Static pressure with sine wave
            pressure = dronecan.uavcan.equipment.air_data.StaticPressure()
            pressure.static_pressure = 101325 + 500 * math.sin(t)
            pressure.static_pressure_variance = 1.0
            node.broadcast(pressure)

            # Temperature with slower sine wave
            temperature = dronecan.uavcan.equipment.air_data.StaticTemperature()
            temperature.static_temperature = 300 + 10 * math.sin(t * 0.5)
            temperature.static_temperature_variance = 0.5
            node.broadcast(temperature)

            # Raw IMU data
            raw_imu = dronecan.uavcan.equipment.ahrs.RawIMU()
            raw_imu.accelerometer_latest = [
                0.1 * math.sin(t * 2),
                0.1 * math.cos(t * 2),
                9.81 + 0.05 * math.sin(t * 3),
            ]
            raw_imu.rate_gyro_latest = [
                0.01 * math.sin(t * 4),
                0.01 * math.cos(t * 4),
                0.005 * math.sin(t),
            ]
            node.broadcast(raw_imu)

            # Battery info with slow discharge
            battery = dronecan.uavcan.equipment.power.BatteryInfo()
            battery.voltage = 25.2 - 0.5 * math.sin(t * 0.1)
            battery.current = 10 + 2 * math.sin(t * 0.3)
            battery.temperature = 305 + 3 * math.sin(t * 0.2)
            battery.state_of_charge_pct = int(80 + 5 * math.sin(t * 0.05))
            node.broadcast(battery)

            node.spin(period)
    except KeyboardInterrupt:
        print('\nStopped')


if __name__ == '__main__':
    main()
