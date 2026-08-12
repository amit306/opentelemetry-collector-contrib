// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/unicode"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/fileconsumer/attrs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/fileconsumer/internal/emittest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/fileconsumer/internal/fingerprint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/fileconsumer/internal/scanner"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/internal/filetest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/split"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/trim"
)

const (
	defaultMaxLogSize  = 1024 * 1024
	defaultFlushPeriod = 500 * time.Millisecond
)

func testFactory(t *testing.T, opts ...testFactoryOpt) (*Factory, *emittest.Sink) {
	cfg := &testFactoryCfg{
		fromBeginning:     true,
		fingerprintSize:   fingerprint.DefaultSize,
		initialBufferSize: scanner.DefaultBufferSize,
		maxLogSize:        defaultMaxLogSize,
		encoding:          unicode.UTF8,
		trimFunc:          trim.Whitespace,
		flushPeriod:       defaultFlushPeriod,
		sinkChanSize:      100,
		attributes: attrs.Resolver{
			IncludeFileName: true,
		},
	}
	for _, opt := range opts {
		opt(cfg)
	}

	splitFunc, err := cfg.splitCfg.Func(cfg.encoding, false, cfg.maxLogSize)
	require.NoError(t, err)

	sink := emittest.NewSink(emittest.WithCallBuffer(cfg.sinkChanSize))
	return &Factory{
		TelemetrySettings:    componenttest.NewNopTelemetrySettings(),
		FromBeginning:        cfg.fromBeginning,
		FingerprintSize:      cfg.fingerprintSize,
		InitialBufferSize:    cfg.initialBufferSize,
		MaxLogSize:           cfg.maxLogSize,
		TruncateOnMaxLogSize: cfg.truncateOnMaxLogSize,
		Encoding:             cfg.encoding,
		SplitFunc:            splitFunc,
		TrimFunc:             cfg.trimFunc,
		FlushTimeout:         cfg.flushPeriod,
		EmitFunc:             sink.Callback,
		Attributes:           cfg.attributes,
		Compression:          cfg.compression,
	}, sink
}

type testFactoryOpt func(*testFactoryCfg)

type testFactoryCfg struct {
	fromBeginning        bool
	fingerprintSize      int
	initialBufferSize    int
	maxLogSize           int
	truncateOnMaxLogSize bool
	encoding             encoding.Encoding
	splitCfg             split.Config
	trimFunc             trim.Func
	flushPeriod          time.Duration
	sinkChanSize         int
	attributes           attrs.Resolver
	compression          string
}

func withFingerprintSize(size int) testFactoryOpt {
	return func(c *testFactoryCfg) {
		c.fingerprintSize = size
	}
}

func withSplitConfig(cfg split.Config) testFactoryOpt {
	return func(c *testFactoryCfg) {
		c.splitCfg = cfg
	}
}

func withInitialBufferSize(size int) testFactoryOpt {
	return func(c *testFactoryCfg) {
		c.initialBufferSize = size
	}
}

func withMaxLogSize(size int) testFactoryOpt {
	return func(c *testFactoryCfg) {
		c.maxLogSize = size
	}
}

func withTruncateOnMaxLogSize(truncate bool) testFactoryOpt {
	return func(c *testFactoryCfg) {
		c.truncateOnMaxLogSize = truncate
	}
}

func withFlushPeriod(flushPeriod time.Duration) testFactoryOpt {
	return func(c *testFactoryCfg) {
		c.flushPeriod = flushPeriod
	}
}

func withSinkChanSize(n int) testFactoryOpt {
	return func(c *testFactoryCfg) {
		c.sinkChanSize = n
	}
}

func fromEnd() testFactoryOpt {
	return func(c *testFactoryCfg) {
		c.fromBeginning = false
	}
}

func withCompression(c string) testFactoryOpt {
	return func(cfg *testFactoryCfg) {
		cfg.compression = c
	}
}

func TestStartAt(t *testing.T) {
	tempDir := t.TempDir()
	temp := filetest.OpenTemp(t, tempDir)
	content := "some text\n"
	_, err := temp.WriteString(content)
	require.NoError(t, err)

	f, _ := testFactory(t, withFingerprintSize(len(content)*2))
	fp, err := f.NewFingerprint(temp)
	require.NoError(t, err)
	r, err := f.NewReader(temp, fp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), r.Offset)

	f, _ = testFactory(t, withFingerprintSize(len(content)/2), fromEnd())
	fp, err = f.NewFingerprint(temp)
	require.NoError(t, err)
	r, err = f.NewReader(temp, fp)
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), r.Offset)

	f, _ = testFactory(t, withFingerprintSize(len(content)/2))
	fp, err = f.NewFingerprint(temp)
	require.NoError(t, err)
	r, err = f.NewReader(temp, fp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), r.Offset)

	f, _ = testFactory(t, withFingerprintSize(len(content)/2), fromEnd())
	fp, err = f.NewFingerprint(temp)
	require.NoError(t, err)
	r, err = f.NewReader(temp, fp)
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), r.Offset)
}

// TestNewReaderFromMetadataConcurrentAccess tests that FileAttributes map
// is not mutated in place, which would cause data races when the metadata
// is accessed concurrently (e.g., by test assertions or persistence).
func TestNewReaderFromMetadataConcurrentAccess(t *testing.T) {
	tempDir := t.TempDir()
	temp := filetest.OpenTemp(t, tempDir)
	content := "test log line\n"
	_, err := temp.WriteString(content)
	require.NoError(t, err)

	f, _ := testFactory(t)
	fp, err := f.NewFingerprint(temp)
	require.NoError(t, err)

	// Create initial reader
	r1, err := f.NewReader(temp, fp)
	require.NoError(t, err)

	// Get metadata (simulating persistence or reader recycling)
	metadata := r1.Close()

	// Store original map reference to detect if it's mutated
	originalAttrs := metadata.FileAttributes
	originalLen := len(originalAttrs)

	// Create a done channel for synchronization
	done := make(chan bool)

	// Goroutine 1: Continuously read the FileAttributes map
	// (simulating test assertions or persistence reading)
	go func() {
		defer func() { done <- true }()
		for range 100 {
			// This simulates what reflect.DeepEqual does in test assertions
			_ = len(originalAttrs)
			for k, v := range originalAttrs {
				_ = k
				_ = v
			}
			time.Sleep(time.Microsecond)
		}
	}()

	// Goroutine 2: Create new reader from metadata
	// (simulating the polling goroutine calling NewReaderFromMetadata)
	go func() {
		defer func() { done <- true }()
		for range 100 {
			temp2 := filetest.OpenFile(t, temp.Name())
			_, err := f.NewReaderFromMetadata(temp2, metadata)
			if err != nil {
				t.Errorf("NewReaderFromMetadata failed: %v", err)
			}
			time.Sleep(time.Microsecond)
		}
	}()

	// Wait for both goroutines to complete
	<-done
	<-done

	// Verify that the original map was not mutated
	// (it should still have the same length, though a new map may have been created)
	require.Len(t, originalAttrs, originalLen,
		"Original FileAttributes map should not be mutated")
}

// TestNewReaderFromMetadataAfterCompression is a regression test for
// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/46105.
//
// When a plaintext log file is compressed (e.g. test.log → test.log.gz),
// fingerprint matching succeeds because the decompressed content of the .gz begins with
// the same bytes as the original plaintext fingerprint. However, the file format has
// changed. NewReaderFromMetadata must re-detect the file type, store the old plaintext
// offset as decompressedBytesToSkip, and zero the persisted offset. createGzipReader
// then decompresses from byte 0 and discards the already-consumed bytes, so only new
// content is emitted — no corruption and no duplicate log entries.
func TestNewReaderFromMetadataAfterCompression(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	lines := []string{
		"{'message': 'Log line 1'}",
		"{'message': 'Log line 2'}",
		"{'message': 'Log line 3'}",
		"{'message': 'Log line 4'}",
	}

	// Create and fully read the original plaintext file (lines 1-2).
	plaintextFile := filetest.OpenTempWithPattern(t, tempDir, "test*.log")
	filetest.WriteString(t, plaintextFile, lines[0]+"\n"+lines[1]+"\n")

	f, sink := testFactory(t, withCompression("auto"))

	fp, err := f.NewFingerprint(plaintextFile)
	require.NoError(t, err)

	r, err := f.NewReader(plaintextFile, fp)
	require.NoError(t, err)
	assert.Empty(t, r.FileType, "plaintext file should have empty FileType")

	r.ReadToEnd(t.Context())
	sink.ExpectTokens(t, []byte(lines[0]), []byte(lines[1]))

	// Save metadata: Offset > 0, FileType == "" (plaintext).
	oldMeta := r.Close()
	require.Positive(t, oldMeta.Offset, "offset should be > 0 after reading plaintext file")
	require.Empty(t, oldMeta.FileType, "old FileType should be empty (plaintext)")

	// Simulate rotation+compression — write a .gz containing 4 lines.
	gzipPath := filepath.Join(tempDir, "test.log.gz")
	gzipFile, err := os.Create(gzipPath)
	require.NoError(t, err)
	gzWriter := gzip.NewWriter(gzipFile)
	_, err = gzWriter.Write([]byte(strings.Join(lines, "\n") + "\n"))
	require.NoError(t, err)
	require.NoError(t, gzWriter.Close())
	require.NoError(t, gzipFile.Close())

	// Open the .gz file and call NewReaderFromMetadata with the old plaintext metadata,
	// exactly as the rotation-matching logic does.
	gzipFileRead, err := os.Open(gzipPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, gzipFileRead.Close()) }()

	newReader, err := f.NewReaderFromMetadata(gzipFileRead, oldMeta)
	require.NoError(t, err)

	// FileType must be updated and Offset must be zeroed (the stale plaintext
	// value is moved into decompressedBytesToSkip, not kept as Offset).
	assert.Equal(t, ".gz", newReader.FileType, "FileType must be updated to .gz after compression")
	assert.Equal(t, int64(0), newReader.Offset, "Offset must be zeroed when transitioning from plaintext to gzip")

	// ReadToEnd must correctly decode only the new lines without corruption or duplicates.
	// The decompressedBytesToSkip mechanism decompresses from byte 0 and discards the
	// bytes for lines 1-2 that were already consumed from the plaintext file.
	newReader.ReadToEnd(t.Context())
	sink.ExpectTokens(t, []byte(lines[2]), []byte(lines[3]))
}

// TestNewReaderFromMetadataFingerprintCollision is a regression test for
// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/50167
func TestNewReaderFromMetadataFingerprintCollision(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	const collisionFingerprintSize = 16

	writeGzipFile := func(name, content string) *os.File {
		path := filepath.Join(tempDir, name)
		gzipFile, err := os.Create(path)
		require.NoError(t, err)
		gzWriter := gzip.NewWriter(gzipFile)
		_, err = gzWriter.Write([]byte(content))
		require.NoError(t, err)
		require.NoError(t, gzWriter.Close())
		require.NoError(t, gzipFile.Close())

		readHandle, err := os.Open(path)
		require.NoError(t, err)
		return readHandle
	}

	// Both files' decompressed content shares the same first 16+ bytes, so
	// under fingerprint_size: 16 the tracker computes an identical
	// fingerprint for both, even though the files are different sizes and
	// diverge after the shared prefix.
	// fileA gets closed by readerA.Close() below; fileB is closed here.
	sharedPrefix := "2026-08-12 startup sequence begins\n"
	fileA := writeGzipFile("a.log.gz", sharedPrefix+"a is a small file\n")
	fileB := writeGzipFile("b.log.gz", sharedPrefix+"b is a much bigger file\nb line 2\nb line 3\nb line 4\n")
	defer func() { require.NoError(t, fileB.Close()) }()

	f, sink := testFactory(t, withCompression("gzip"), withFingerprintSize(collisionFingerprintSize))

	core, obs := observer.New(zapcore.DebugLevel)
	f.Logger = zap.New(core)

	fpA, err := f.NewFingerprint(fileA)
	require.NoError(t, err)
	fpB, err := f.NewFingerprint(fileB)
	require.NoError(t, err)
	require.True(t, fpA.Equal(fpB), "fingerprints must collide for this test to be meaningful")

	// Fully read file A, exactly like the tracker would on the first poll.
	readerA, err := f.NewReader(fileA, fpA)
	require.NoError(t, err)
	readerA.ReadToEnd(t.Context())
	sink.ExpectTokens(t, []byte("2026-08-12 startup sequence begins"), []byte("a is a small file"))

	// Close it to get the Metadata the tracker would save: Offset now points
	// to the end of a.log.gz's compressed bytes, and FileType is ".gz".
	oldMeta := readerA.Close()
	require.Positive(t, oldMeta.Offset)
	require.Equal(t, ".gz", oldMeta.FileType)

	// Simulate the tracker matching file B against file A's saved metadata,
	// because their fingerprints are identical - this is the call the
	// tracker makes after GetClosedFile / MatchStartsWith succeeds.
	readerB, err := f.NewReaderFromMetadata(fileB, oldMeta)
	require.NoError(t, err)

	// This is the bug: because FileType didn't change (".gz" -> ".gz"), the
	// stale Offset from file A is carried straight into file B's reader,
	// instead of being reset the way the plaintext-to-gzip case is above.
	require.Equal(t, oldMeta.Offset, readerB.Offset,
		"file B inherited file A's Offset, which does not point to a valid position in file B's own gzip stream")

	// ReadToEnd should now fail to decode file B, because readerB.Offset does
	// not land on a valid gzip header inside file B's compressed bytes.
	readerB.ReadToEnd(t.Context())
	sink.ExpectNoCallsUntil(t, 200*time.Millisecond)

	foundExpectedError := false
	for _, entry := range obs.All() {
		if entry.Level != zapcore.ErrorLevel || entry.Message != "failed to create gzip reader" {
			continue
		}
		for _, field := range entry.Context {
			if field.Key == "error" {
				if err, ok := field.Interface.(error); ok && strings.Contains(err.Error(), "invalid header") {
					foundExpectedError = true
				}
			}
		}
	}
	require.True(t, foundExpectedError,
		"expected a gzip 'invalid header' error caused by the borrowed offset not pointing to a valid gzip header in file B")
}
