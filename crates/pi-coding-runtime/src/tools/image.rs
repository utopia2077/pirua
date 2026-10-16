//! Local vision-input normalization. Image generation is outside this module.
use super::{EnvResult, ProcessedImage};
use ::image::{DynamicImage, ImageDecoder, ImageFormat, ImageReader, imageops::FilterType};
use base64::Engine;
use std::io::Cursor;

pub(super) fn process(
    bytes: &[u8],
    mime: &str,
    options: &super::ReadToolOptions,
) -> EnvResult<Option<ProcessedImage>> {
    let auto_resize = options.auto_resize;
    let resize = options.resize.clone().unwrap_or_default();
    let max_width = resize
        .max_width
        .map_or(2000, |value| value.get().min(u32::MAX as u64) as u32);
    let max_height = resize
        .max_height
        .map_or(2000, |value| value.get().min(u32::MAX as u64) as u32);
    let max_payload = resize.max_bytes.map_or(4_718_592, |value| {
        usize::try_from(value.get()).unwrap_or(usize::MAX)
    });
    let quality = resize
        .jpeg_quality
        .map_or(80, |value| value.get().clamp(1, 100) as u8);
    let processed = (|| {
        if auto_resize && (max_width == 0 || max_height == 0) {
            return None;
        }
        if !auto_resize && mime != "image/bmp" {
            return Some(ProcessedImage {
                data: base64::engine::general_purpose::STANDARD.encode(bytes),
                mime_type: mime.into(),
                hints: Vec::new(),
            });
        }
        let mut decoder = ImageReader::new(Cursor::new(bytes))
            .with_guessed_format()
            .ok()?
            .into_decoder()
            .ok()?;
        let orientation = decoder.orientation().ok();
        let mut image = DynamicImage::from_decoder(decoder).ok()?;
        if let Some(orientation) = orientation {
            image.apply_orientation(orientation);
        }
        let (original_width, original_height) = (image.width(), image.height());
        if mime != "image/bmp"
            && original_width <= max_width
            && original_height <= max_height
            && bytes.len().div_ceil(3) * 4 < max_payload
        {
            return Some(ProcessedImage {
                data: base64::engine::general_purpose::STANDARD.encode(bytes),
                mime_type: mime.into(),
                hints: Vec::new(),
            });
        }
        let mut width = original_width;
        let mut height = original_height;
        if auto_resize {
            if width > max_width {
                height = (height as f64 * max_width as f64 / width as f64)
                    .round()
                    .max(1.0) as u32;
                width = max_width;
            }
            if height > max_height {
                width = (width as f64 * max_height as f64 / height as f64)
                    .round()
                    .max(1.0) as u32;
                height = max_height;
            }
        }
        loop {
            let resized = image.resize_exact(width, height, FilterType::Lanczos3);
            let mut png = Cursor::new(Vec::new());
            resized.write_to(&mut png, ImageFormat::Png).ok()?;
            let mut candidate = (png.into_inner(), "image/png");
            if auto_resize && candidate.0.len().div_ceil(3) * 4 >= max_payload {
                for quality in [quality, 85, 70, 55, 40] {
                    let mut jpeg = Vec::new();
                    ::image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, quality)
                        .encode_image(&resized.to_rgb8())
                        .ok()?;
                    if jpeg.len().div_ceil(3) * 4 < max_payload {
                        candidate = (jpeg, "image/jpeg");
                        break;
                    }
                }
            }
            if !auto_resize || candidate.0.len().div_ceil(3) * 4 < max_payload {
                let mut hints = if width != original_width || height != original_height {
                    vec![format!(
                        "[Image: original {original_width}x{original_height}, displayed at {width}x{height}. Multiply coordinates by {:.2} to map to original image.]",
                        original_width as f64 / width as f64
                    )]
                } else {
                    Vec::new()
                };
                if mime == "image/bmp" {
                    hints.insert(
                        0,
                        format!("[Image converted from image/bmp to {}.]", candidate.1),
                    );
                }
                return Some(ProcessedImage {
                    data: base64::engine::general_purpose::STANDARD.encode(candidate.0),
                    mime_type: candidate.1.into(),
                    hints,
                });
            }
            if !auto_resize || (width == 1 && height == 1) {
                return None;
            }
            width = ((width as f64 * 0.75).floor() as u32).max(1);
            height = ((height as f64 * 0.75).floor() as u32).max(1);
        }
    })();
    Ok(processed)
}
