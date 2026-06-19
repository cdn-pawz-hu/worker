package encoder

import (
	"fmt"
	"os/exec"
)

type FFmpegEncoder struct{}

func NewFFmpegEncoder() *FFmpegEncoder {
	return &FFmpegEncoder{}
}

// TranscodeTo1080pH264 spawns a system subprocess to run FFmpeg natively
func (f *FFmpegEncoder) TranscodeTo1080pH264(inputPath, outputPath string) error {
	// -y forces overwrite without asking questions
	// -vf scale=-2:1080 scales height to 1080 while forcing width to be divisible by 2 (required by H.264)
	cmd := exec.Command("ffmpeg",
		"-y",
		"-i", inputPath,
		"-c:v", "libx264",
		"-preset", "fast",
		"-crf", "23",
		"-vf", "scale=-2:1080",
		"-c:a", "aac",
		"-b:a", "128k",
		outputPath,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg execution failed: %w, logs: %s", err, string(output))
	}

	return nil
}
