<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Utils;

/**
 * Buffered file writer for memory-efficient, high-performance writes
 *
 * This class provides a memory-safe way to write large amounts of data
 * by accumulating lines in a buffer and flushing to disk in batches.
 * This dramatically reduces the number of syscalls compared to writing
 * line-by-line.
 *
 * Benefits:
 * - Much fewer I/O operations than line-by-line fwrite() (~1000x fewer)
 * - Bounded memory usage (only one buffer in memory at a time)
 * - Automatic flushing when buffer reaches threshold
 *
 * Example: Writing 100,000 lines
 * - Individual fwrite(): 100,000 syscalls
 * - BufferedFileWriter (buffer=1000): ~100 syscalls
 *
 * @package Spiritinlife\MapReduce
 */
class BufferedFileWriter
{
    /** @var resource|null */
    private $handle;
    /** @var array<int, string> */
    private array $buffer = [];
    private int $bufferSize;
    private string $filename;

    /**
     * Create a new buffered file writer
     *
     * @param string $filename File to write to
     * @param int $bufferSize Number of lines to buffer before flushing (default: 1000)
     * @throws \RuntimeException If file cannot be opened
     */
    public function __construct(string $filename, int $bufferSize = 1000)
    {
        $this->filename = $filename;
        $this->bufferSize = $bufferSize;
        $handle = @fopen($filename, 'w');

        if ($handle === false) {
            throw new \RuntimeException("Failed to open file for buffered writing: $filename");
        }
        $this->handle = $handle;
    }

    /**
     * Write a line to the buffer
     *
     * The line will be buffered and flushed to disk when the buffer
     * reaches the configured size, or when flush() is called explicitly.
     *
     * @param string $line Line to write (should include trailing newline if desired)
     * @return void
     * @throws \RuntimeException If write fails
     */
    public function writeLine(string $line): void
    {
        $this->buffer[] = $line;

        // Auto-flush when buffer is full
        if (count($this->buffer) >= $this->bufferSize) {
            $this->flush();
        }
    }

    /**
     * Flush the buffer to disk
     *
     * Writes all buffered lines to the file and clears the buffer.
     * This is called automatically when the buffer is full, but can
     * also be called manually to ensure data is written to disk.
     *
     * @return void
     * @throws \RuntimeException If write fails
     */
    public function flush(): void
    {
        if (empty($this->buffer) || $this->handle === null) {
            return;
        }

        $data = implode('', $this->buffer);
        $written = fwrite($this->handle, $data);

        if ($written === false) {
            throw new \RuntimeException("Failed to write to file: {$this->filename}");
        }

        $this->buffer = [];
    }

    /**
     * Close the file handle
     *
     * Flushes any remaining buffered data and closes the file.
     *
     * @return void
     */
    public function close(): void
    {
        if ($this->handle !== null) {
            $this->flush();
            if ($this->handle !== null) {
                fclose($this->handle);
            }
            $this->handle = null;
        }
    }

    /**
     * Get the file path
     *
     * @return string File path
     */
    public function getFilename(): string
    {
        return $this->filename;
    }

    /**
     * Destructor ensures file handle is closed and data is flushed
     */
    public function __destruct()
    {
        $this->close();
    }
}
