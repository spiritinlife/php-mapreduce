<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Utils;

/**
 * Buffered file reader for memory-efficient line-by-line reading
 *
 * This class provides a memory-safe way to read large files by loading
 * them in configurable chunks (e.g., 64KB) rather than all at once.
 * It maintains a small in-memory buffer that gets refilled as needed.
 *
 * Benefits:
 * - Much fewer I/O operations than fgets() (~100x fewer for large files)
 * - Bounded memory usage (only one chunk in memory at a time)
 * - Avoids loading entire multi-megabyte files into memory
 *
 * Example: 10MB file with 100,000 lines
 * - fgets(): 100,000 I/O operations
 * - file_get_contents(): 1 I/O, 10MB memory
 * - BufferedFileReader: ~160 I/O operations, 64KB memory
 *
 * @package Spiritinlife\MapReduce
 */
class BufferedFileReader
{
    /** @var resource|null */
    private $handle;
    /** @var array<int, string> */
    private array $buffer = [];
    private int $bufferIndex = 0;
    private int $readChunkSize;
    private bool $eof = false;
    private string $partialLine = '';

    /**
     * Create a new buffered file reader
     *
     * @param string $filename File to read
     * @param int $readChunkSize Bytes to read per chunk (default: 64KB)
     * @throws \RuntimeException If file cannot be opened
     */
    public function __construct(string $filename, int $readChunkSize = 65536)
    {
        $handle = @fopen($filename, 'r');
        if ($handle === false) {
            throw new \RuntimeException("Failed to open file for buffered reading: $filename");
        }
        $this->handle = $handle;

        $this->readChunkSize = $readChunkSize;
        $this->fillBuffer();
    }

    /**
     * Get the next line from the file
     *
     * @return string|null Next line or null if EOF
     */
    public function getLine(): ?string
    {
        // Return from buffer if available
        if ($this->bufferIndex < count($this->buffer)) {
            return $this->buffer[$this->bufferIndex++];
        }

        // Keep refilling buffer until we get data or reach EOF
        // This handles cases where a line spans multiple chunks
        while (!$this->eof) {
            $this->fillBuffer();

            if ($this->bufferIndex < count($this->buffer)) {
                return $this->buffer[$this->bufferIndex++];
            }
        }

        // No more data
        return null;
    }

    /**
     * Check if there are more lines available
     *
     * @return bool True if more lines available
     */
    public function hasLines(): bool
    {
        return $this->bufferIndex < count($this->buffer) || !$this->eof;
    }

    /**
     * Fill the internal buffer with next chunk of lines
     *
     * Reads readChunkSize bytes (e.g., 64KB), splits into lines,
     * and handles partial lines at chunk boundaries.
     */
    private function fillBuffer(): void
    {
        if ($this->eof || $this->handle === null) {
            return;
        }

        // Read a chunk of bytes (e.g., 64KB)
        $chunk = fread($this->handle, max(1, $this->readChunkSize));

        if ($chunk === false || $chunk === '') {
            // Handle any remaining partial line
            if ($this->partialLine !== '') {
                $this->buffer = [$this->partialLine];
                $this->partialLine = '';
            } else {
                $this->buffer = [];
            }
            $this->bufferIndex = 0;
            $this->eof = true;
            return;
        }

        // Combine with any partial line from previous chunk
        $chunk = $this->partialLine . $chunk;

        // Split into lines
        $lines = explode("\n", $chunk);

        // Last element might be incomplete (no trailing \n yet)
        // Save it for the next chunk
        $lastLine = array_pop($lines);
        /** @phpstan-ignore-next-line */
        if ($lastLine === null) {
            $this->partialLine = '';
        } else {
            $this->partialLine = $lastLine;
        }

        // Check if we're at EOF
        if (feof($this->handle)) {
            // Add the final partial line if it exists
            if ($this->partialLine !== '') {
                $lines[] = $this->partialLine;
                $this->partialLine = '';
            }
            $this->eof = true;
        }

        // Filter empty lines and reset buffer
        $this->buffer = array_filter($lines, fn($line) => $line !== '');
        $this->buffer = array_values($this->buffer); // Re-index
        $this->bufferIndex = 0;
    }

    /**
     * Close the file handle
     */
    public function close(): void
    {
        if ($this->handle) {
            fclose($this->handle);
            $this->handle = null;
        }
    }

    /**
     * Destructor ensures file handle is closed
     */
    public function __destruct()
    {
        $this->close();
    }
}
