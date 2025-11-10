<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Utils;

/**
 * CSV file writer for memory-efficient streaming of reduce results
 *
 * This class provides a memory-safe way to write reduce results directly to CSV
 * without accumulating them in memory. Results are buffered and written in batches
 * for optimal I/O performance.
 *
 * Benefits:
 * - Process arbitrarily large result sets without memory constraints
 * - Reduce phase no longer needs to hold all results in memory
 * - Results available immediately in CSV format
 * - Buffered writes reduce I/O syscalls by ~1000x compared to line-by-line
 *
 * @package Spiritinlife\MapReduce
 */
class CsvFileWriter
{
    /** @var resource|null */
    private $handle;
    /** @var array<int, string> */
    private array $buffer = [];
    private int $bufferSize;
    private string $filename;
    private bool $headerWritten = false;
    /** @var array<int, string> */
    private array $headers = [];

    /**
     * Create a new CSV file writer
     *
     * @param string $filename File to write to
     * @param int $bufferSize Number of rows to buffer before flushing (default: 1000)
     * @throws \RuntimeException If file cannot be opened
     */
    public function __construct(string $filename, int $bufferSize = 1000)
    {
        $this->filename = $filename;
        $this->bufferSize = $bufferSize;
        $handle = @fopen($filename, 'w');

        if ($handle === false) {
            throw new \RuntimeException("Failed to open file for CSV writing: $filename");
        }
        $this->handle = $handle;
    }

    /**
     * Write CSV header row
     *
     * Must be called before writing data rows.
     *
     * @param array<int|string, string> $headers Column headers
     * @return void
     * @throws \RuntimeException If write fails
     */
    public function writeHeader(array $headers): void
    {
        if ($this->headerWritten) {
            throw new \RuntimeException("Header already written to CSV");
        }

        $this->headers = array_values($headers);
        $csvLine = $this->encodeRow($this->headers);
        $this->buffer[] = $csvLine . "\n";
        $this->headerWritten = true;

        // Auto-flush when buffer is full
        if (count($this->buffer) >= $this->bufferSize) {
            $this->flush();
        }
    }

    /**
     * Write a data row to the CSV
     *
     * Row can be associative or numeric array. Values are automatically
     * quoted and escaped according to CSV standards.
     *
     * @param array<mixed> $row Row data
     * @return void
     * @throws \RuntimeException If write fails
     */
    public function writeRow(array $row): void
    {
        if (!$this->headerWritten) {
            // Auto-detect headers from first row if not explicitly set
            $this->writeHeader(array_keys($row));
        }

        // Convert row to string values, reordering if needed
        $rowValues = [];
        foreach ($this->headers as $header) {
            if (isset($row[$header])) {
                $value = $row[$header];
            } else {
                $value = '';
            }
            $rowValues[] = $this->castToString($value);
        }

        $csvLine = $this->encodeRow($rowValues);
        $this->buffer[] = $csvLine . "\n";

        // Auto-flush when buffer is full
        if (count($this->buffer) >= $this->bufferSize) {
            $this->flush();
        }
    }

    /**
     * Flush the buffer to disk
     *
     * Writes all buffered rows to the file and clears the buffer.
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
            throw new \RuntimeException("Failed to write to CSV file: {$this->filename}");
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
     * Encode a row as CSV line
     *
     * Follows RFC 4180 CSV standard:
     * - Fields containing quotes, commas, or newlines are quoted
     * - Quotes within fields are escaped by doubling
     *
     * @param array<int|string, string> $row Row to encode
     * @return string CSV line without trailing newline
     */
    private function encodeRow(array $row): string
    {
        $encoded = [];
        foreach ($row as $field) {
            if (strpos($field, ',') !== false || strpos($field, '"') !== false || strpos($field, "\n") !== false) {
                // Quote and escape field
                $encoded[] = '"' . str_replace('"', '""', $field) . '"';
            } else {
                $encoded[] = $field;
            }
        }
        return implode(',', $encoded);
    }

    /**
     * Cast value to string for CSV output
     *
     * @param mixed $value Value to cast
     * @return string String representation
     */
    private function castToString(mixed $value): string
    {
        if ($value === null) {
            return '';
        }
        if (is_bool($value)) {
            return $value ? '1' : '0';
        }
        if (is_array($value) || is_object($value)) {
            $encoded = json_encode($value);
            return $encoded !== false ? $encoded : '{}';
        }
        return (string) $value;
    }

    /**
     * Destructor ensures file handle is closed and data is flushed
     */
    public function __destruct()
    {
        $this->close();
    }
}
