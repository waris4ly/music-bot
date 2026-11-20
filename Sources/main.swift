import DiscordBM
import DiscordGateway
import DiscordHTTP
import DiscordModels
import DiscordVoice
import Foundation
import Logging

let logger = Logger(label: "discordbot")

// MARK: - Opus Audio Provider & FFmpeg Transcoder
/// A custom implementation of `OpusFrameProvider` that pipes `yt-dlp` into `ffmpeg`
/// and parses the resulting Ogg Opus container into 20ms Opus frames for Discord.
actor FFmpegOpusPlayer: OpusFrameProvider {
  private var queue: [[UInt8]] = []
  private var isFinished = false
  private var ytdlpProcess: Process?
  private var ffmpegProcess: Process?
  private var fileHandle: FileHandle?
  private var readBuffer = [UInt8]()
  private var currentPacket = [UInt8]()
  let title: String

  init(title: String) {
    self.title = title
  }

  /// Spawns the `yt-dlp` -> `ffmpeg` pipeline connected via Unix pipes.
  func start(target: String) throws {
    // Step 1: yt-dlp process streams best audio to stdout
    let ytdlp = Process()
    ytdlp.executableURL = URL(fileURLWithPath: "/opt/homebrew/bin/yt-dlp")
    ytdlp.arguments = [
      "--no-playlist",
      "--quiet",
      "-f", "bestaudio",
      "-o", "-",
      target,
    ]

    // Step 2: ffmpeg process reads from pipe:0 and outputs Ogg Opus to pipe:1
    let ffmpeg = Process()
    ffmpeg.executableURL = URL(fileURLWithPath: "/opt/homebrew/bin/ffmpeg")
    ffmpeg.arguments = [
      "-i", "pipe:0",
      "-c:a", "libopus",
      "-b:a", "128k",
      "-ar", "48000",
      "-ac", "2",
      "-page_duration", "20000",  // Exactly 20ms per Ogg page (matches Discord RTP packet size)
      "-f", "opus",
      "pipe:1",
    ]

    let interPipe = Pipe()
    let outPipe = Pipe()

    // Chain yt-dlp stdout -> ffmpeg stdin
    ytdlp.standardOutput = interPipe
    ytdlp.standardError = FileHandle.nullDevice

    ffmpeg.standardInput = interPipe
    ffmpeg.standardOutput = outPipe
    ffmpeg.standardError = FileHandle.nullDevice

    let handle = outPipe.fileHandleForReading
    self.fileHandle = handle
    self.ytdlpProcess = ytdlp
    self.ffmpegProcess = ffmpeg

    // Step 3: Asynchronously read chunks from ffmpeg stdout without blocking NIOCore threads
    handle.readabilityHandler = { [weak self] h in
      let data = h.availableData
      if data.isEmpty {
        // EOF reached: clear handler immediately to prevent spin on macOS
        h.readabilityHandler = nil
        Task { [weak self] in
          await self?.finishReading()
        }
      } else {
        let bytes = Array(data)
        Task { [weak self] in
          await self?.ingestChunk(bytes)
        }
      }
    }

    try ytdlp.run()
    try ffmpeg.run()
    logger.info("Started streaming pipeline", metadata: ["title": .string(title)])
  }

  private func ingestChunk(_ chunk: [UInt8]) {
    readBuffer.append(contentsOf: chunk)
    parseOggPages()
  }

  /// Demuxes Ogg pages according to RFC 3533.
  /// Each page has a 27-byte header + segment table + page payload.
  /// Packets ending with segment length < 255 are complete Opus frames.
  private func parseOggPages() {
    while true {
      guard readBuffer.count >= 27 else { break }

      // Sync with Ogg magic bytes "OggS" (0x4F, 0x67, 0x67, 0x53)
      if readBuffer[0] != 0x4F || readBuffer[1] != 0x67 || readBuffer[2] != 0x67
        || readBuffer[3] != 0x53
      {
        readBuffer.removeFirst()
        continue
      }

      let numSegments = Int(readBuffer[26])
      let headerSize = 27 + numSegments
      guard readBuffer.count >= headerSize else { break }

      var bodyLength = 0
      for i in 0..<numSegments {
        bodyLength += Int(readBuffer[27 + i])
      }

      let totalPageSize = headerSize + bodyLength
      guard readBuffer.count >= totalPageSize else { break }

      let segmentTable = Array(readBuffer[27..<headerSize])
      let body = Array(readBuffer[headerSize..<totalPageSize])
      readBuffer.removeFirst(totalPageSize)

      var bodyOffset = 0
      for segLen in segmentTable {
        let segSize = Int(segLen)
        currentPacket.append(contentsOf: body[bodyOffset..<(bodyOffset + segSize)])
        bodyOffset += segSize

        // Segment length < 255 marks the end of an Opus packet
        if segLen < 255 {
          if !currentPacket.isEmpty && !Self.isHeaderPacket(currentPacket) {
            queue.append(currentPacket)
          }
          currentPacket.removeAll(keepingCapacity: true)
        }
      }
    }
  }

  /// Filters out Ogg Opus metadata packets (identification and comment headers).
  private static func isHeaderPacket(_ pkt: [UInt8]) -> Bool {
    guard pkt.count >= 8 else { return false }
    let prefix = String(decoding: pkt.prefix(8), as: UTF8.self)
    return prefix == "OpusHead" || prefix == "OpusTags"
  }

  private func finishReading() {
    guard !isFinished else { return }
    parseOggPages()
    if !currentPacket.isEmpty && !Self.isHeaderPacket(currentPacket) {
      queue.append(currentPacket)
      currentPacket.removeAll()
    }
    isFinished = true
    fileHandle?.readabilityHandler = nil
    logger.info("Completed stream input", metadata: ["title": .string(title)])
  }

  /// Terminates both worker processes and cleans up resources.
  func stop() {
    isFinished = true
    queue.removeAll()
    fileHandle?.readabilityHandler = nil
    if let y = ytdlpProcess, y.isRunning {
      y.terminate()
    }
    if let f = ffmpegProcess, f.isRunning {
      f.terminate()
    }
    ytdlpProcess = nil
    ffmpegProcess = nil
    fileHandle = nil
  }

  /// Called by `AudioSender` every 20ms to fetch the next audio packet.
  func nextFrame() async -> [UInt8]? {
    if !queue.isEmpty {
      return queue.removeFirst()
    }
    if isFinished {
      return nil
    }
    // If transcoder is momentarily buffering, wait briefly (up to 25ms) before yielding
    for _ in 0..<5 {
      try? await Task.sleep(for: .milliseconds(5))
      if !queue.isEmpty {
        return queue.removeFirst()
      }
      if isFinished {
        return nil
      }
    }
    return nil
  }
}

// MARK: - YouTube Resolution Service
/// Resolves search queries or direct URLs to clean video titles using `yt-dlp`.
enum YTDLPService {
  static func resolve(query: String) async throws -> (title: String, target: String) {
    let target =
      query.hasPrefix("http://") || query.hasPrefix("https://")
      ? query
      : "ytsearch1:\(query)"

    let proc = Process()
    proc.executableURL = URL(fileURLWithPath: "/opt/homebrew/bin/yt-dlp")
    proc.arguments = [
      "--print", "%(title)s",
      "--no-playlist",
      target,
    ]

    let pipe = Pipe()
    proc.standardOutput = pipe
    proc.standardError = FileHandle.nullDevice

    try proc.run()
    proc.waitUntilExit()

    let data = pipe.fileHandleForReading.readDataToEndOfFile()
    guard
      let output = String(data: data, encoding: .utf8)?.trimmingCharacters(
        in: .whitespacesAndNewlines),
      !output.isEmpty
    else {
      throw NSError(
        domain: "YTDLP",
        code: 1,
        userInfo: [NSLocalizedDescriptionKey: "No results found for query."]
      )
    }

    let title = output.components(separatedBy: "\n").first ?? output
    return (title: title, target: target)
  }
}

// MARK: - Inbound Audio Receiver
/// Consumes decrypted per-user Opus audio frames from Discord participants.
/// Inbound pipeline: UDP socket -> Transport Decryptor -> DAVE MLS Decryptor -> OpusFrameReceiver.
final class InboundAudioLogger: OpusFrameReceiver, @unchecked Sendable {
  private var count = 0

  func receive(frame: [UInt8], from userID: UserSnowflake) async {
    count += 1
    if count <= 3 || count % 100 == 0 {
      logger.info(
        "Inbound audio packet received",
        metadata: [
          "user_id": .string(userID.rawValue),
          "frame_count": .stringConvertible(count),
          "bytes": .stringConvertible(frame.count),
        ]
      )
    }
  }
}

// MARK: - Bot State Actor
/// Thread-safe state container tracking voice channels and active players across guilds.
actor BotState {
  private var userVoiceChannels: [UserSnowflake: ChannelSnowflake] = [:]
  private var activePlayers: [GuildSnowflake: FFmpegOpusPlayer] = [:]

  func setVoiceChannel(_ channelID: ChannelSnowflake?, for user: UserSnowflake) {
    if let channelID {
      userVoiceChannels[user] = channelID
    } else {
      userVoiceChannels.removeValue(forKey: user)
    }
  }

  func getVoiceChannel(for user: UserSnowflake) -> ChannelSnowflake? {
    userVoiceChannels[user]
  }

  func setPlayer(_ player: FFmpegOpusPlayer?, for guildID: GuildSnowflake) -> FFmpegOpusPlayer? {
    let old = activePlayers[guildID]
    if let player {
      activePlayers[guildID] = player
    } else {
      activePlayers.removeValue(forKey: guildID)
    }
    return old
  }

  func getPlayer(for guildID: GuildSnowflake) -> FFmpegOpusPlayer? {
    activePlayers[guildID]
  }
}

// MARK: - Bot Application Entry Point
@main
struct DiscordVoiceBot {
  static func main() async throws {
    LoggingSystem.bootstrap { label in
      var handler = StreamLogHandler.standardOutput(label: label)
      handler.logLevel = .info
      return handler
    }

    guard let token = ProcessInfo.processInfo.environment["BOT_TOKEN"], !token.isEmpty else {
      fatalError("BOT_TOKEN environment variable is required.")
    }

    let bot = await BotGatewayManager(
      token: token,
      intents: [.guilds, .guildMessages, .messageContent, .guildVoiceStates]
    )

    let client = bot.client
    let currentUserID = try await client.getOwnUser().decode().id

    // VoiceManager routes Discord Voice Gateway handshakes and tracks VoiceConnection instances
    let voiceManager = VoiceManager(
      gateway: bot,
      userID: currentUserID
    )

    let state = BotState()

    await bot.connect()
    logger.info(
      "Connected to Discord Gateway", metadata: ["bot_user_id": .string(currentUserID.rawValue)])

    // Register Discord Slash Commands globally
    try await registerSlashCommands(client: client)

    // Main event loop: forward voice events to VoiceManager and handle interactions concurrently
    let events = await bot.events
    for await event in events {
      await voiceManager.handleEvent(event)

      switch event.data {
      case .guildCreate(let guild):
        for vs in guild.voice_states {
          if let cid = vs.channel_id {
            await state.setVoiceChannel(cid, for: vs.user_id)
          }
        }

      case .voiceStateUpdate(let voiceState):
        await state.setVoiceChannel(voiceState.channel_id, for: voiceState.user_id)

      case .interactionCreate(let interaction):
        // Execute in detached Task so Voice Gateway events are never blocked
        Task {
          await handleInteraction(
            interaction, client: client, voiceManager: voiceManager, state: state)
        }

      case .messageCreate(let message):
        // Text prefix fallback handler
        Task {
          await handleMessage(message, client: client, voiceManager: voiceManager, state: state)
        }

      default:
        break
      }
    }
  }

  /// Registers the bot's slash commands with the Discord API.
  private static func registerSlashCommands(client: any DiscordClient) async throws {
    let commands: [Payloads.ApplicationCommandCreate] = [
      .init(
        name: "play",
        description: "Play audio from YouTube in your voice channel",
        options: [
          .init(
            type: .string,
            name: "query",
            description: "Song title or YouTube URL",
            required: true
          )
        ]
      ),
      .init(
        name: "stop",
        description: "Stop audio playback"
      ),
      .init(
        name: "leave",
        description: "Disconnect from voice channel"
      ),
      .init(
        name: "listen",
        description: "Listen to incoming voice packets"
      ),
      .init(
        name: "help",
        description: "Show available commands"
      ),
    ]

    _ = try await client.bulkSetApplicationCommands(payload: commands)
    logger.info(
      "Registered global application commands",
      metadata: ["count": .stringConvertible(commands.count)])
  }

  /// Handles incoming Discord Slash Command interactions.
  private static func handleInteraction(
    _ interaction: Interaction,
    client: any DiscordClient,
    voiceManager: VoiceManager,
    state: BotState
  ) async {
    guard let data = interaction.data,
      case .applicationCommand(let cmd) = data,
      let guildID = interaction.guild_id
    else { return }

    let userID = interaction.member?.user?.id ?? interaction.user?.id
    guard let userID else { return }

    switch cmd.name {
    case "help":
      _ = try? await client.createInteractionResponse(
        id: interaction.id,
        token: interaction.token,
        payload: .channelMessageWithSource(
          .init(
            content: """
              **DiscordVoice Bot Commands**:
              • `/play <query>` - Stream audio from YouTube with full pipeline diagnostics
              • `/stop` - Stop audio playback
              • `/leave` - Disconnect from voice channel
              • `/listen` - Receive and log incoming decrypted voice frames
              • `/help` - Show command information
              """
          )
        )
      )

    case "play":
      guard let channelID = await state.getVoiceChannel(for: userID) else {
        _ = try? await client.createInteractionResponse(
          id: interaction.id,
          token: interaction.token,
          payload: .channelMessageWithSource(
            .init(content: "You must be in a voice channel to use this command.")
          )
        )
        return
      }

      guard let query = try? cmd.requireOption(named: "query").requireString(), !query.isEmpty
      else {
        _ = try? await client.createInteractionResponse(
          id: interaction.id,
          token: interaction.token,
          payload: .channelMessageWithSource(.init(content: "Please provide a valid query."))
        )
        return
      }

      // Defer interaction response while resolving and establishing audio pipeline
      _ = try? await client.createInteractionResponse(
        id: interaction.id,
        token: interaction.token,
        payload: .deferredChannelMessageWithSource()
      )

      do {
        // Step 1: Resolve metadata via yt-dlp
        let (title, target) = try await YTDLPService.resolve(query: query)

        // Step 2: Connect to voice channel and complete DAVE MLS handshake
        let connection = try await voiceManager.join(guildID: guildID, channelID: channelID)

        // Step 3: Stop existing player if any
        if let existing = await state.setPlayer(nil, for: guildID) {
          await existing.stop()
        }

        // Step 4: Launch piped yt-dlp -> ffmpeg process
        let player = FFmpegOpusPlayer(title: title)
        try await player.start(target: target)
        _ = await state.setPlayer(player, for: guildID)

        // Step 5: Attach OpusFrameProvider to VoiceConnection for real-time UDP streaming
        await connection.setFrameProvider(player)

        // Format comprehensive pipeline status breakdown
        let responseContent = """
          **Now Playing**: **\(title)**

          **Voice Pipeline Execution Details**:
          1. **Query Resolution**: Resolved `\(query)` via `yt-dlp`
          2. **Voice Channel**: Joined <#\(channelID.rawValue)> via `VoiceManager.join`
          3. **Security Handshake**: Negotiated Discord DAVE MLS E2EE session & ratchets
          4. **Audio Transcoding**: `yt-dlp` pipe -> `ffmpeg` (48kHz stereo Opus @ 128kbps, 20ms page duration)
          5. **UDP Transmission**: Attached `FFmpegOpusPlayer` to `AudioSender` (20ms paced packets)
          """

        _ = try? await client.updateOriginalInteractionResponse(
          token: interaction.token,
          payload: .init(content: responseContent)
        )
      } catch {
        _ = try? await client.updateOriginalInteractionResponse(
          token: interaction.token,
          payload: .init(content: "Failed to play audio: \(error.localizedDescription)")
        )
      }

    case "stop":
      if let player = await state.setPlayer(nil, for: guildID) {
        await player.stop()
        if let conn = await voiceManager.connection(for: guildID) {
          await conn.setFrameProvider(nil)
        }
        _ = try? await client.createInteractionResponse(
          id: interaction.id,
          token: interaction.token,
          payload: .channelMessageWithSource(.init(content: "Playback stopped."))
        )
      } else {
        _ = try? await client.createInteractionResponse(
          id: interaction.id,
          token: interaction.token,
          payload: .channelMessageWithSource(.init(content: "No audio is currently playing."))
        )
      }

    case "leave":
      if let player = await state.setPlayer(nil, for: guildID) {
        await player.stop()
      }
      await voiceManager.leave(guildID: guildID)
      _ = try? await client.createInteractionResponse(
        id: interaction.id,
        token: interaction.token,
        payload: .channelMessageWithSource(.init(content: "Disconnected from voice channel."))
      )

    case "listen":
      guard let channelID = await state.getVoiceChannel(for: userID) else {
        _ = try? await client.createInteractionResponse(
          id: interaction.id,
          token: interaction.token,
          payload: .channelMessageWithSource(
            .init(content: "You must be in a voice channel to use this command.")
          )
        )
        return
      }

      do {
        let conn = try await voiceManager.join(guildID: guildID, channelID: channelID)
        let receiver = InboundAudioLogger()
        await conn.setFrameReceiver(receiver)
        _ = try? await client.createInteractionResponse(
          id: interaction.id,
          token: interaction.token,
          payload: .channelMessageWithSource(
            .init(content: "Listening for incoming voice packets in <#\(channelID.rawValue)>.")
          )
        )
      } catch {
        _ = try? await client.createInteractionResponse(
          id: interaction.id,
          token: interaction.token,
          payload: .channelMessageWithSource(
            .init(content: "Failed to connect: \(error.localizedDescription)")
          )
        )
      }

    default:
      break
    }
  }

  /// Handles text-based prefix commands (.play, .stop, etc.) as a fallback.
  private static func handleMessage(
    _ message: Gateway.MessageCreate,
    client: any DiscordClient,
    voiceManager: VoiceManager,
    state: BotState
  ) async {
    guard let author = message.author, author.bot != true, let guildID = message.guild_id else {
      return
    }
    let text = message.content.trimmingCharacters(in: .whitespacesAndNewlines)

    if text == ".help" {
      _ = try? await client.createMessage(
        channelId: message.channel_id,
        payload: .init(
          content: """
            **DiscordVoice Bot Commands**:
            • `.play <query>` - Stream audio from YouTube with full pipeline diagnostics
            • `.stop` - Stop audio playback
            • `.leave` - Disconnect from voice channel
            • `.listen` - Receive and log incoming voice frames
            • `.help` - Show command information
            """
        )
      )
    } else if text.hasPrefix(".play") {
      let query = text.dropFirst(5).trimmingCharacters(in: .whitespaces)
      guard !query.isEmpty else {
        _ = try? await client.createMessage(
          channelId: message.channel_id,
          payload: .init(content: "Please provide a query: `.play <query>`")
        )
        return
      }

      guard let channelID = await state.getVoiceChannel(for: author.id) else {
        _ = try? await client.createMessage(
          channelId: message.channel_id,
          payload: .init(content: "You must be in a voice channel first.")
        )
        return
      }

      _ = try? await client.createMessage(
        channelId: message.channel_id,
        payload: .init(content: "Searching: '\(query)'")
      )

      do {
        let (title, target) = try await YTDLPService.resolve(query: query)
        let connection = try await voiceManager.join(guildID: guildID, channelID: channelID)

        if let existing = await state.setPlayer(nil, for: guildID) {
          await existing.stop()
        }

        let player = FFmpegOpusPlayer(title: title)
        try await player.start(target: target)
        _ = await state.setPlayer(player, for: guildID)
        await connection.setFrameProvider(player)

        let responseContent = """
          **Now Playing**: **\(title)**

          **Voice Pipeline Execution Details**:
          1. **Query Resolution**: Resolved `\(query)` via `yt-dlp`
          2. **Voice Channel**: Joined <#\(channelID.rawValue)> via `VoiceManager.join`
          3. **Security Handshake**: Negotiated Discord DAVE MLS E2EE session & ratchets
          4. **Audio Transcoding**: `yt-dlp` pipe -> `ffmpeg` (48kHz stereo Opus @ 128kbps, 20ms page duration)
          5. **UDP Transmission**: Attached `FFmpegOpusPlayer` to `AudioSender` (20ms paced packets)
          """

        _ = try? await client.createMessage(
          channelId: message.channel_id,
          payload: .init(content: responseContent)
        )
      } catch {
        _ = try? await client.createMessage(
          channelId: message.channel_id,
          payload: .init(content: "Playback error: \(error.localizedDescription)")
        )
      }
    } else if text == ".stop" {
      if let player = await state.setPlayer(nil, for: guildID) {
        await player.stop()
        if let conn = await voiceManager.connection(for: guildID) {
          await conn.setFrameProvider(nil)
        }
        _ = try? await client.createMessage(
          channelId: message.channel_id,
          payload: .init(content: "Playback stopped.")
        )
      } else {
        _ = try? await client.createMessage(
          channelId: message.channel_id,
          payload: .init(content: "No audio is currently playing.")
        )
      }
    } else if text == ".leave" {
      if let player = await state.setPlayer(nil, for: guildID) {
        await player.stop()
      }
      await voiceManager.leave(guildID: guildID)
      _ = try? await client.createMessage(
        channelId: message.channel_id,
        payload: .init(content: "Disconnected from voice channel.")
      )
    } else if text == ".listen" {
      guard let channelID = await state.getVoiceChannel(for: author.id) else {
        _ = try? await client.createMessage(
          channelId: message.channel_id,
          payload: .init(content: "You must be in a voice channel first.")
        )
        return
      }

      do {
        let conn = try await voiceManager.join(guildID: guildID, channelID: channelID)
        let receiver = InboundAudioLogger()
        await conn.setFrameReceiver(receiver)
        _ = try? await client.createMessage(
          channelId: message.channel_id,
          payload: .init(
            content: "Listening for incoming voice packets in <#\(channelID.rawValue)>.")
        )
      } catch {
        _ = try? await client.createMessage(
          channelId: message.channel_id,
          payload: .init(content: "Connection error: \(error.localizedDescription)")
        )
      }
    }
  }
}
