// swift-tools-version: 6.0
import PackageDescription

let package = Package(
  name: "discordbot",
  platforms: [
    .macOS(.v13)
  ],
  dependencies: [
    .package(url: "https://github.com/waris4ly/DiscordBM", branch: "feat/discord-voice"),
    .package(url: "https://github.com/apple/swift-log.git", from: "1.5.0"),
  ],
  targets: [
    .executableTarget(
      name: "discordbot",
      dependencies: [
        .product(name: "DiscordBM", package: "DiscordBM"),
        .product(name: "DiscordVoice", package: "DiscordBM"),
        .product(name: "Logging", package: "swift-log"),
      ],
      path: "Sources"
    )
  ]
)
