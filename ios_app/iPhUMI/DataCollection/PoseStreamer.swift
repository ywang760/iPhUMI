//
//  PoseStreamer.swift
//  iPhUMI
//
//  Streams the ARKit camera pose of every frame to a computer over USB, so an external
//  recorder (e.g. an OAK camera recorder on the computer) can use the iPhone as a live pose
//  source. The computer connects to `port` on the iPhone through usbmuxd, the same way the
//  deployment USB streams work. The stream runs whenever the data collection screen is open
//  and is independent of on-phone recording.
//
//  Wire format, little-endian; every message is a header followed by its payload:
//    header: u32 magic 'UMP1' (0x31504D55), u8 type, u8 reserved, u16 payload length
//    type 1 POSE (56 bytes): u64 seq, i64 frame_ts_ns (ARFrame.timestamp),
//        i64 send_ns (ProcessInfo.systemUptime at send), f32 tx ty tz qx qy qz qw
//        (ARFrame.camera.transform: world from camera), u8 tracking_state
//        (0 normal, 1 limited, 2 not available), u8 limited reason
//        (1 initializing, 2 excessive motion, 3 insufficient features, 4 relocalizing),
//        u16 session_epoch (changes when tracking is reset)
//    type 2 INFO: UTF-8 JSON (device, iOS and app versions, image size, intrinsics)
//    type 3 PREVIEW: u64 seq, u16 w, u16 h, w*h gray bytes (luma plane, about 5 Hz)
//

import ARKit
import Foundation
import Network
import UIKit

final class PoseStreamer {
    static let defaultPort: UInt16 = 5560
    private static let magic: UInt32 = 0x31504D55
    private static let typePose: UInt8 = 1
    private static let typeInfo: UInt8 = 2
    private static let typePreview: UInt8 = 3
    private static let previewWidth = 160
    private static let previewPeriod: TimeInterval = 0.2
    private static let maxPendingSends = 240

    let port: UInt16

    // Socket state, confined to `queue`.
    private let queue = DispatchQueue(label: "PoseStreamer")
    private var listener: NWListener?
    private var connection: NWConnection?
    private var pendingSends = 0
    private var lastInfo: Data?

    // Read from the ARSession delegate thread.
    private let lock = NSLock()
    private var connectedFlag = false

    // Frame state, touched only from the ARSession delegate (main) thread.
    private var seq: UInt64 = 0
    private var epoch: UInt16 = 0
    private var infoSentForEpoch = false
    private var lastPreviewUptime: TimeInterval = 0

    init(port: UInt16 = PoseStreamer.defaultPort) {
        self.port = port
    }

    var isConnected: Bool {
        lock.lock()
        defer { lock.unlock() }
        return connectedFlag
    }

    private func setConnected(_ value: Bool) {
        lock.lock()
        connectedFlag = value
        lock.unlock()
    }

    // MARK: Lifecycle

    func start() {
        queue.async {
            guard self.listener == nil, let nwPort = NWEndpoint.Port(rawValue: self.port) else { return }
            do {
                let params = NWParameters.tcp
                params.allowLocalEndpointReuse = true
                let listener = try NWListener(using: params, on: nwPort)
                listener.newConnectionHandler = { [weak self] newConnection in
                    self?.accept(newConnection)
                }
                listener.stateUpdateHandler = { state in
                    if case .failed(let error) = state {
                        print("PoseStreamer: listener failed: \(error)")
                    }
                }
                listener.start(queue: self.queue)
                self.listener = listener
                print("PoseStreamer: listening on port \(self.port)")
            } catch {
                print("PoseStreamer: cannot listen on port \(self.port): \(error)")
            }
        }
    }

    func stop() {
        queue.async {
            self.connection?.cancel()
            self.connection = nil
            self.listener?.cancel()
            self.listener = nil
            self.setConnected(false)
        }
    }

    /// Call whenever ARKit tracking (and so the world origin) is reset.
    func bumpEpoch() {
        epoch &+= 1
        infoSentForEpoch = false
    }

    // Runs on `queue`. One client at a time; a new client replaces the old one.
    private func accept(_ newConnection: NWConnection) {
        connection?.cancel()
        connection = newConnection
        pendingSends = 0
        newConnection.stateUpdateHandler = { [weak self, weak newConnection] state in
            guard let self = self, let conn = newConnection else { return }
            switch state {
            case .ready:
                print("PoseStreamer: client connected")
                self.setConnected(true)
                if let info = self.lastInfo {
                    self.sendOnQueue(info, to: conn)
                }
            case .failed, .cancelled:
                if self.connection === conn {
                    self.connection = nil
                    self.setConnected(false)
                    print("PoseStreamer: client disconnected")
                }
            default:
                break
            }
        }
        newConnection.start(queue: queue)
    }

    // MARK: Sending

    /// Call from session(_:didUpdate:) for every ARFrame.
    func send(frame: ARFrame) {
        let thisSeq = seq
        seq &+= 1
        guard isConnected else { return }

        if !infoSentForEpoch {
            setInfo(makeInfo(frame: frame))
            infoSentForEpoch = true
        }

        let camera = frame.camera
        let transform = camera.transform
        let q = simd_quatf(transform)
        var state: UInt8 = 2
        var reason: UInt8 = 0
        if case .normal = camera.trackingState {
            state = 0
        } else if case .limited(let limitedReason) = camera.trackingState {
            state = 1
            reason = PoseStreamer.limitedReasonCode(limitedReason)
        }

        var payload = Data(capacity: 56)
        PoseStreamer.appendLE(&payload, thisSeq)
        PoseStreamer.appendLE(&payload, Int64(frame.timestamp * 1e9))
        PoseStreamer.appendLE(&payload, Int64(ProcessInfo.processInfo.systemUptime * 1e9))
        PoseStreamer.appendF32(&payload, transform.columns.3.x)
        PoseStreamer.appendF32(&payload, transform.columns.3.y)
        PoseStreamer.appendF32(&payload, transform.columns.3.z)
        PoseStreamer.appendF32(&payload, q.imag.x)
        PoseStreamer.appendF32(&payload, q.imag.y)
        PoseStreamer.appendF32(&payload, q.imag.z)
        PoseStreamer.appendF32(&payload, q.real)
        payload.append(state)
        payload.append(reason)
        PoseStreamer.appendLE(&payload, epoch)
        enqueue(PoseStreamer.message(type: PoseStreamer.typePose, payload: payload))

        let now = ProcessInfo.processInfo.systemUptime
        if now - lastPreviewUptime >= PoseStreamer.previewPeriod {
            lastPreviewUptime = now
            if let preview = makePreview(frame.capturedImage, seq: thisSeq) {
                enqueue(PoseStreamer.message(type: PoseStreamer.typePreview, payload: preview))
            }
        }
    }

    private func setInfo(_ json: Data) {
        let message = PoseStreamer.message(type: PoseStreamer.typeInfo, payload: json)
        queue.async {
            self.lastInfo = message
        }
        enqueue(message)
    }

    private func enqueue(_ message: Data) {
        queue.async {
            guard let conn = self.connection else { return }
            self.sendOnQueue(message, to: conn)
        }
    }

    // Runs on `queue`. Drops messages instead of buffering without bound if the computer stalls.
    private func sendOnQueue(_ message: Data, to conn: NWConnection) {
        if pendingSends >= PoseStreamer.maxPendingSends {
            return
        }
        pendingSends += 1
        conn.send(content: message, completion: .contentProcessed { [weak self] error in
            guard let self = self else { return }
            self.pendingSends = max(0, self.pendingSends - 1)
            if error != nil {
                conn.cancel()
            }
        })
    }

    // MARK: Encoding

    private static func message(type messageType: UInt8, payload: Data) -> Data {
        var data = Data(capacity: 8 + payload.count)
        appendLE(&data, magic)
        data.append(messageType)
        data.append(0)
        appendLE(&data, UInt16(clamping: payload.count))
        data.append(payload)
        return data
    }

    private static func appendLE<T: FixedWidthInteger>(_ data: inout Data, _ value: T) {
        withUnsafeBytes(of: value.littleEndian) { data.append(contentsOf: $0) }
    }

    private static func appendF32(_ data: inout Data, _ value: Float) {
        appendLE(&data, value.bitPattern)
    }

    private static func limitedReasonCode(_ reason: ARCamera.TrackingState.Reason) -> UInt8 {
        switch reason {
        case .initializing:
            return 1
        case .excessiveMotion:
            return 2
        case .insufficientFeatures:
            return 3
        case .relocalizing:
            return 4
        @unknown default:
            return 255
        }
    }

    private func makeInfo(frame: ARFrame) -> Data {
        let k = frame.camera.intrinsics  // column-major: k[0][0] fx, k[1][1] fy, k[2][0] cx, k[2][1] cy
        let resolution = frame.camera.imageResolution
        let width = Int(resolution.width)
        let height = Int(resolution.height)
        let bundle = Bundle.main.infoDictionary
        let appVersion = "\(bundle?["CFBundleShortVersionString"] as? String ?? "?") (\(bundle?["CFBundleVersion"] as? String ?? "?"))"
        let info: [String: Any] = [
            "app": "iPhUMI",
            "app_version": appVersion,
            "manufacturer": "Apple",
            "model": PoseStreamer.machineIdentifier(),
            "device": UIDevice.current.model,
            "ios_version": UIDevice.current.systemVersion,
            "session_epoch": Int(epoch),
            "camera_config": ["image_size": ["width": width, "height": height] as [String: Any]] as [String: Any],
            "image_intrinsics": [
                "fx": Double(k[0][0]), "fy": Double(k[1][1]),
                "cx": Double(k[2][0]), "cy": Double(k[2][1]),
                "width": width, "height": height,
            ] as [String: Any],
            "pose_frame": "ARFrame.camera.transform: world from camera; ARKit world Y up (gravity); camera +X right, +Y up, -Z view direction in the sensor's native landscape orientation",
            "timestamp_clock": "frame_ts_ns = ARFrame.timestamp, send_ns = ProcessInfo.systemUptime (same clock)",
        ]
        return (try? JSONSerialization.data(withJSONObject: info)) ?? Data("{}".utf8)
    }

    private static func machineIdentifier() -> String {
        var systemInfo = utsname()
        uname(&systemInfo)
        return withUnsafeBytes(of: &systemInfo.machine) { raw in
            String(decoding: raw.prefix(while: { $0 != 0 }), as: UTF8.self)
        }
    }

    /// Downscaled copy of the luma plane of the camera image, for the computer's live view.
    private func makePreview(_ buffer: CVPixelBuffer, seq: UInt64) -> Data? {
        CVPixelBufferLockBaseAddress(buffer, .readOnly)
        defer { CVPixelBufferUnlockBaseAddress(buffer, .readOnly) }
        guard CVPixelBufferGetPlaneCount(buffer) >= 1,
              let base = CVPixelBufferGetBaseAddressOfPlane(buffer, 0) else { return nil }
        let width = CVPixelBufferGetWidthOfPlane(buffer, 0)
        let height = CVPixelBufferGetHeightOfPlane(buffer, 0)
        let rowBytes = CVPixelBufferGetBytesPerRowOfPlane(buffer, 0)
        let step = max(1, width / PoseStreamer.previewWidth)
        let w = width / step
        let h = height / step
        let src = base.assumingMemoryBound(to: UInt8.self)
        var gray = [UInt8](repeating: 0, count: w * h)
        for r in 0..<h {
            let row = src + r * step * rowBytes
            for c in 0..<w {
                gray[r * w + c] = row[c * step]
            }
        }
        var payload = Data(capacity: 12 + gray.count)
        PoseStreamer.appendLE(&payload, seq)
        PoseStreamer.appendLE(&payload, UInt16(w))
        PoseStreamer.appendLE(&payload, UInt16(h))
        payload.append(contentsOf: gray)
        return payload
    }
}
