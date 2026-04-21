// Pipeline.swift

import Foundation

enum Direction {
    case forward
    case hold
    case drop
    case branch
}

struct Packet<T> {
    let id: String
    let content: T
    let meta: [String: Any]
    let created: TimeInterval
    
    init(id: String, content: T, meta: [String: Any] = [:], created: TimeInterval = Date().timeIntervalSince1970) {
        self.id = id
        self.content = content
        self.meta = meta
        self.created = created
    }
    
    func replace<U>(newContent: U) -> Packet<U> {
        return Packet<U>(
            id: self.id,
            content: newContent,
            meta: self.meta,
            created: self.created
        )
    }
    
    func annotate(key: String, value: Any) -> Packet<T> {
        var newMeta = self.meta
        newMeta[key] = value
        return Packet(
            id: self.id,
            content: self.content,
            meta: newMeta,
            created: self.created
        )
    }
}

struct Batch<T> {
    let entries: [Packet<T>]
    let batchId: String
    
    init(entries: [Packet<T>], batchId: String = UUID().uuidString.prefix(12).description) {
        self.entries = entries
        self.batchId = batchId
    }
    
    func count() -> Int { return entries.count }
    func empty() -> Bool { return entries.isEmpty }
}

struct Result<U> {
    let direction: Direction
    let output: Batch<U>?
    let error: String?
    
    static func proceed(batch: Batch<U>) -> Result<U> {
        return Result(direction: .forward, output: batch, error: nil)
    }
    
    static func terminate() -> Result<U> {
        return Result(direction: .drop, output: nil, error: nil)
    }
    
    static func wait(reason: String) -> Result<U> {
        return Result(direction: .hold, output: nil, error: reason)
    }
    
    static func split() -> Result<U> {
        return Result(direction: .branch, output: nil, error: nil)
    }
}

protocol Stage {
    associatedtype Input
    associatedtype Output
    func run(incoming: Batch<Input>) -> Result<Output>
}

struct Collector<T> {
    var pending: [Packet<T>] = []
    var partitions: [String: [Packet<T>]] = [:]
    
    mutating func add(packet: Packet<T>) {
        pending.append(packet)
        let key = packet.meta["partition"] as? String ?? "default"
        if partitions[key] == nil {
            partitions[key] = []
        }
        partitions[key]?.append(packet)
    }
    
    mutating func collect() -> [Packet<[Packet<T>]>] {
        var results: [Packet<[Packet<T>]>] = []
        for (key, items) in partitions {
            if !items.isEmpty {
                let merged = Packet<[Packet<T>]>(
                    id: "merge_\(key)_\(Int(Date().timeIntervalSince1970))",
                    content: items,
                    meta: ["partition": key, "size": items.count]
                )
                results.append(merged)
            }
        }
        partitions.removeAll()
        pending.removeAll()
        return results
    }
}

class Aggregator<T>: Stage {
    typealias Input = T
    typealias Output = [Packet<T>]
    
    let limit: Int
    var collector: Collector<T>
    
    init(limit: Int = 100) {
        self.limit = limit
        self.collector = Collector()
    }
    
    func run(incoming: Batch<T>) -> Result<[Packet<T>]> {
        for p in incoming.entries {
            collector.add(packet: p)
        }
        
        if collector.pending.count >= limit {
            let merged = collector.collect()
            if !merged.isEmpty {
                return .proceed(batch: Batch(entries: merged))
            }
        }
        
        return .proceed(batch: Batch(entries: []))
    }
}

class Mapper<T, U>: Stage {
    typealias Input = T
    typealias Output = U
    
    let transform: (T) throws -> U
    
    init(transform: @escaping (T) throws -> U) {
        self.transform = transform
    }
    
    func run(incoming: Batch<T>) -> Result<U> {
        var mapped: [Packet<U>] = []
        for p in incoming.entries {
            do {
                let newContent = try transform(p.content)
                mapped.append(p.replace(newContent: newContent))
            } catch {
                return .wait(reason: "transform_failed")
            }
        }
        
        if mapped.isEmpty {
            return .split()
        }
        
        return .proceed(batch: Batch(entries: mapped))
    }
}

class Condition<T>: Stage {
    typealias Input = T
    typealias Output = T
    
    let predicate: (T) -> Bool
    
    init(predicate: @escaping (T) -> Bool) {
        self.predicate = predicate
    }
    
    func run(incoming: Batch<T>) -> Result<T> {
        let filtered = incoming.entries.filter { predicate($0.content) }
        
        if filtered.isEmpty {
            return .split()
        }
        
        return .proceed(batch: Batch(entries: filtered))
    }
}

class Router<T>: Stage {
    typealias Input = T
    typealias Output = T
    
    let paths: [String: (T) -> Bool]
    
    init(paths: [String: @escaping (T) -> Bool]) {
        self.paths = paths
    }
    
    func run(incoming: Batch<T>) -> Result<T> {
        var routed: [String: [Packet<T>]] = [:]
        for name in paths.keys {
            routed[name] = []
        }
        routed["_default"] = []
        
        for p in incoming.entries {
            var matched = false
            for (name, test) in paths {
                if test(p.content) {
                    routed[name]?.append(p)
                    matched = true
                    break
                }
            }
            if !matched {
                routed["_default"]?.append(p)
            }
        }
        
        var allItems: [Packet<T>] = []
        for (name, items) in routed {
            if let items = items, !items.isEmpty {
                for item in items {
                    allItems.append(item.annotate(key: "route", value: name))
                }
            }
        }
        
        if allItems.isEmpty {
            return .split()
        }
        
        return .proceed(batch: Batch(entries: allItems))
    }
}

class KernelInterface: Stage {
    typealias Input = Int
    typealias Output = [String: Any]
    
    func run(incoming: Batch<Int>) -> Result<[String: Any]> {
        var results: [Packet<[String: Any]>] = []
        for p in incoming.entries {
            let pid = p.content
            if pid > 0 && pid < 1000 {
                let taskPort = self.getTaskPort(pid)
                if taskPort != 0 {
                    let regions = self.enumerateRegions(taskPort)
                    results.append(p.replace(newContent: ["task": taskPort, "regions": regions]))
                }
            }
        }
        return .proceed(batch: Batch(entries: results))
    }
    
    private func getTaskPort(_ pid: Int) -> UInt32 { return 0 }
    private func enumerateRegions(_ task: UInt32) -> [[String: Any]] { return [] }
}

class MemoryInterface: Stage {
    typealias Input = (task: UInt32, address: UInt64)
    typealias Output = Data
    
    func run(incoming: Batch<(task: UInt32, address: UInt64)>) -> Result<Data> {
        var results: [Packet<Data>] = []
        for p in incoming.entries {
            let (task, address) = p.content
            if let data = self.readMemory(task: task, address: address, size: 4096) {
                results.append(p.replace(newContent: data))
            }
        }
        return .proceed(batch: Batch(entries: results))
    }
    
    private func readMemory(task: UInt32, address: UInt64, size: UInt64) -> Data? { return nil }
}

class PortInterface: Stage {
    typealias Input = UInt32
    typealias Output = (send: Bool, receive: Bool, once: Bool)
    
    func run(incoming: Batch<UInt32>) -> Result<(send: Bool, receive: Bool, once: Bool)> {
        var results: [Packet<(send: Bool, receive: Bool, once: Bool)>] = []
        for p in incoming.entries {
            let rights = self.extractRights(port: p.content)
            results.append(p.replace(newContent: rights))
        }
        return .proceed(batch: Batch(entries: results))
    }
    
    private func extractRights(port: UInt32) -> (Bool, Bool, Bool) { return (false, false, false) }
}

class ThreadInterface: Stage {
    typealias Input = UInt32
    typealias Output = [UInt32]
    
    func run(incoming: Batch<UInt32>) -> Result<[UInt32]> {
        var results: [Packet<[UInt32]>] = []
        for p in incoming.entries {
            let threads = self.enumerateThreads(task: p.content)
            results.append(p.replace(newContent: threads))
        }
        return .proceed(batch: Batch(entries: results))
    }
    
    private func enumerateThreads(task: UInt32) -> [UInt32] { return [] }
}

class SymbolInterface: Stage {
    typealias Input = String
    typealias Output = UInt64
    
    func run(incoming: Batch<String>) -> Result<UInt64> {
        var results: [Packet<UInt64>] = []
        for p in incoming.entries {
            if let address = self.resolveSymbol(name: p.content) {
                results.append(p.replace(newContent: address))
            }
        }
        return .proceed(batch: Batch(entries: results))
    }
    
    private func resolveSymbol(name: String) -> UInt64? { return nil }
}

class HookInterface: Stage {
    typealias Input = (target: UInt64, replacement: UInt64)
    typealias Output = Bool
    
    func run(incoming: Batch<(target: UInt64, replacement: UInt64)>) -> Result<Bool> {
        var results: [Packet<Bool>] = []
        for p in incoming.entries {
            let installed = self.installHook(target: p.content.target, replacement: p.content.replacement)
            results.append(p.replace(newContent: installed))
        }
        return .proceed(batch: Batch(entries: results))
    }
    
    private func installHook(target: UInt64, replacement: UInt64) -> Bool { return false }
}

struct PipelineNode {
    let stage: Any
    var next: [PipelineNode]
    
    func execute<T, U>(data: Batch<T>) -> [Result<Any>] where U: Any {
        guard let typedStage = stage as? any Stage else {
            return [.terminate()]
        }
        
        let result = typedStage.run(incoming: data) as! Result<U>
        
        guard result.direction == .forward, let output = result.output, !output.empty() else {
            return [result]
        }
        
        var results: [Result<Any>] = [result]
        for nextNode in next {
            let nextResults = nextNode.execute(data: output)
            results.append(contentsOf: nextResults)
        }
        return results
    }
}

class Flow {
    let entry: PipelineNode
    
    init(entry: PipelineNode) {
        self.entry = entry
    }
    
    func start<T>(initial: Batch<T>) -> [Result<Any>] {
        return entry.execute(data: initial)
    }
}

class Source<T>: Stage {
    typealias Input = Void
    typealias Output = T
    
    let generator: () -> [T]
    
    init(generator: @escaping () -> [T]) {
        self.generator = generator
    }
    
    func run(incoming: Batch<Void>) -> Result<T> {
        let raw = generator()
        let packets = raw.map { Packet(id: UUID().uuidString, content: $0) }
        return .proceed(batch: Batch(entries: packets))
    }
}

class Sink<T>: Stage {
    typealias Input = T
    typealias Output = Void
    
    let consumer: ([Packet<T>]) -> Void
    
    init(consumer: @escaping ([Packet<T>]) -> Void) {
        self.consumer = consumer
    }
    
    func run(incoming: Batch<T>) -> Result<Void> {
        consumer(incoming.entries)
        return .proceed(batch: Batch(entries: []))
    }
}

func generatePids() -> [Int] {
    return stride(from: 1, to: 500, by: 10).map { $0 }
}

func isSystem(pid: Int) -> Bool { return pid < 50 }
func isUser(pid: Int) -> Bool { return 100 <= pid && pid < 400 }

func logOutput(items: [Packet<Int>]) {
    let path = NSTemporaryDirectory() + "flow.log"
    let content = items.map { "\($0.content)" }.joined(separator: "\n")
    try? content.write(toFile: path, atomically: true, encoding: .utf8)
}

let source = Source(generator: generatePids)
let kernel = KernelInterface()
let filterSystem = Condition(predicate: isSystem)
let filterUser = Condition(predicate: isUser)
let router = Router(paths: ["system": isSystem, "user": isUser])
let memory = MemoryInterface()
let port = PortInterface()
let symbol = SymbolInterface()
let hook = HookInterface()
let sink = Sink(consumer: logOutput)

let nodeSink = PipelineNode(stage: sink, next: [])
let nodeHook = PipelineNode(stage: hook, next: [nodeSink])
let nodeSymbol = PipelineNode(stage: symbol, next: [nodeHook])
let nodePort = PipelineNode(stage: port, next: [nodeSymbol])
let nodeMemory = PipelineNode(stage: memory, next: [nodePort])
let nodeRouter = PipelineNode(stage: router, next: [nodeMemory])
let nodeUser = PipelineNode(stage: filterUser, next: [nodeRouter])
let nodeKernel = PipelineNode(stage: kernel, next: [nodeUser])
let nodeSource = PipelineNode(stage: source, next: [nodeKernel])

let flow = Flow(entry: nodeSource)
let _ = flow.start(initial: Batch(entries: []))
