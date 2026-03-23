import AppKit
import Foundation

let args = CommandLine.arguments
guard args.count >= 3 else {
    fputs("usage: swift build_emoji_icon.swift <iconset_dir> <emoji>\n", stderr)
    exit(1)
}

let iconsetURL = URL(fileURLWithPath: args[1], isDirectory: true)
let emoji = args[2]
let fileManager = FileManager.default

try? fileManager.removeItem(at: iconsetURL)
try fileManager.createDirectory(at: iconsetURL, withIntermediateDirectories: true)

let sizes: [(Int, String)] = [
    (16, "icon_16x16.png"),
    (32, "icon_16x16@2x.png"),
    (32, "icon_32x32.png"),
    (64, "icon_32x32@2x.png"),
    (128, "icon_128x128.png"),
    (256, "icon_128x128@2x.png"),
    (256, "icon_256x256.png"),
    (512, "icon_256x256@2x.png"),
    (512, "icon_512x512.png"),
    (1024, "icon_512x512@2x.png"),
]

func drawIcon(size: Int) -> NSImage {
    let image = NSImage(size: NSSize(width: size, height: size))
    image.lockFocus()

    let rect = NSRect(x: 0, y: 0, width: size, height: size)
    let background = NSBezierPath(roundedRect: rect, xRadius: CGFloat(size) * 0.22, yRadius: CGFloat(size) * 0.22)
    NSColor(calibratedRed: 0.07, green: 0.09, blue: 0.11, alpha: 1.0).setFill()
    background.fill()

    let glowRect = rect.insetBy(dx: CGFloat(size) * 0.06, dy: CGFloat(size) * 0.06)
    let glowPath = NSBezierPath(ovalIn: glowRect)
    NSColor(calibratedRed: 1.0, green: 0.54, blue: 0.22, alpha: 0.18).setFill()
    glowPath.fill()

    let shadow = NSShadow()
    shadow.shadowBlurRadius = CGFloat(size) * 0.04
    shadow.shadowColor = NSColor(calibratedWhite: 0.0, alpha: 0.35)
    shadow.shadowOffset = NSSize(width: 0, height: -CGFloat(size) * 0.02)
    shadow.set()

    let font = NSFont(name: "Apple Color Emoji", size: CGFloat(size) * 0.62) ?? NSFont.systemFont(ofSize: CGFloat(size) * 0.62)
    let paragraph = NSMutableParagraphStyle()
    paragraph.alignment = .center
    let attributes: [NSAttributedString.Key: Any] = [
        .font: font,
        .paragraphStyle: paragraph,
    ]
    let attributed = NSAttributedString(string: emoji, attributes: attributes)
    let textHeight = attributed.size().height
    let textRect = NSRect(
        x: 0,
        y: CGFloat(size) * 0.50 - textHeight * 0.56,
        width: CGFloat(size),
        height: textHeight * 1.2
    )
    attributed.draw(in: textRect)

    image.unlockFocus()
    return image
}

for (size, name) in sizes {
    let image = drawIcon(size: size)
    guard
        let tiff = image.tiffRepresentation,
        let bitmap = NSBitmapImageRep(data: tiff),
        let pngData = bitmap.representation(using: .png, properties: [:])
    else {
        fputs("failed to render \(name)\n", stderr)
        exit(1)
    }
    try pngData.write(to: iconsetURL.appendingPathComponent(name))
}
