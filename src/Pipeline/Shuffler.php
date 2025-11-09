<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Pipeline;

use Spiritinlife\MapReduce\Utils\BufferedFileReader;
use Spiritinlife\MapReduce\Utils\BufferedFileWriter;
use Spiritinlife\MapReduce\Utils\MergeMinHeap;

use function Amp\async;
use function Amp\Future\await;

/**
 * Shuffler component for MapReduce pipeline
 *
 * This class implements a memory-efficient, disk-based shuffle phase that processes
 * data in chunks, avoiding loading entire datasets into memory. It uses external
 * sorting to merge and group intermediate key-value pairs.
 *
 * @package Spiritinlife\MapReduce
 */
class Shuffler
{
    private string $workingDir;
    private int $chunkSize;
    private int $bufferSize;

    /**
     * Create a new Shuffler
     *
     * @param string $workingDir Directory for temporary files
     * @param int $chunkSize Number of records to process in each chunk
     * @param int $bufferSize Number of records to buffer before flushing (default: 1000)
     */
    public function __construct(
        string $workingDir,
        int $chunkSize = 10000,
        int $bufferSize = 1000
    ) {
        $this->workingDir = $workingDir;
        $this->chunkSize = $chunkSize;
        $this->bufferSize = $bufferSize;
    }

    /**
     * Shuffle multiple partitions in parallel
     *
     * @param array<int, array<int, string>> $mapOutputFiles Map output files grouped by partition
     * @param int $numPartitions Number of partitions
     * @return array<int, string> Shuffled files (one per partition)
     */
    public function shuffle(array $mapOutputFiles, int $numPartitions): array
    {
        // Create async tasks for each partition shuffle
        $futures = [];
        for ($partition = 0; $partition < $numPartitions; $partition++) {
            $inputFiles = $mapOutputFiles[$partition] ?? [];
            $futures[] = async(function () use ($partition, $inputFiles) {
                return $this->shufflePartition($partition, $inputFiles);
            });
        }

        // Wait for all partitions to complete
        return await($futures);
    }

    /**
     * Shuffle a partition using chunk-based external sorting
     *
     * @param int $partition Partition number
     * @param array<int, string> $inputFiles List of input files to shuffle
     * @return string Path to the shuffled output file
     */
    public function shufflePartition(int $partition, array $inputFiles): string
    {
        if (empty($inputFiles)) {
            // Create empty shuffled file
            $shuffledFile = "{$this->workingDir}/shuffled_{$partition}.tmp";
            touch($shuffledFile);
            return $shuffledFile;
        }

        // Phase 1: Sort input files into chunks
        $sortedChunkFiles = $this->sortChunks($partition, $inputFiles);

        // Phase 2: Merge sorted chunks with grouping
        $shuffledFile = $this->mergeChunks($partition, $sortedChunkFiles);

        // Cleanup chunk files
        foreach ($sortedChunkFiles as $chunkFile) {
            @unlink($chunkFile);
        }

        return $shuffledFile;
    }

    /**
     * Sort input files into manageable chunks
     *
     * @param int $partition Partition number
     * @param array<int, string> $inputFiles Input files to process
     * @return array<int, string> List of sorted chunk files
     */
    private function sortChunks(int $partition, array $inputFiles): array
    {
        $chunkFiles = [];
        $chunkIndex = 0;
        $chunk = [];
        $recordCount = 0;

        foreach ($inputFiles as $filename) {
            if (!file_exists($filename)) {
                continue;
            }

            // Always use buffered reading for consistency and memory safety
            $reader = new BufferedFileReader($filename);
            $lines = [];

            while (($line = $reader->getLine()) !== null) {
                $lines[] = $line;
            }
            $reader->close();

            $this->processLines($lines, $partition, $chunkIndex, $chunk, $recordCount, $chunkFiles);
        }

        return $chunkFiles;
    }

    /**
     * Process lines and write chunks
     *
     * @param array<int, string> $lines Lines to process
     * @param int $partition Partition number
     * @param int &$chunkIndex Current chunk index (passed by reference)
     * @param array<int, array{serialized_key: string, original_key: mixed, value: mixed}> &$chunk Current chunk buffer (passed by reference)
     * @param int &$recordCount Current record count (passed by reference)
     * @param array<int, string> &$chunkFiles List of chunk files (passed by reference)
     */
    private function processLines(
        array $lines,
        int $partition,
        int &$chunkIndex,
        array &$chunk,
        int &$recordCount,
        array &$chunkFiles
    ): void {
        foreach ($lines as $line) {
            // Skip empty lines
            if (empty($line)) {
                continue;
            }

            $pair = unserialize($line);
            $chunk[] = [
                'serialized_key' => $this->serializeKey($pair['key']),
                'original_key' => $pair['key'],
                'value' => $pair['value']
            ];

            $recordCount++;

            // When chunk is full, sort and write it
            if ($recordCount >= $this->chunkSize) {
                $chunkFile = $this->writeChunk($partition, $chunkIndex, $chunk);
                $chunkFiles[] = $chunkFile;
                $chunkIndex++;

                $chunk = [];
                $recordCount = 0;
            }
        }

        // Write remaining records as final chunk
        if (!empty($chunk)) {
            $chunkFile = $this->writeChunk($partition, $chunkIndex, $chunk);
            $chunkFiles[] = $chunkFile;
            $chunkIndex++;

            // Reset for next file
            $chunk = [];
            $recordCount = 0;
        }
    }

    /**
     * Write a sorted chunk to disk
     *
     * @param int $partition Partition number
     * @param int $chunkIndex Chunk index
     * @param array<int, array{serialized_key: string, original_key: mixed, value: mixed}> $chunk Records to write
     * @return string Path to the chunk file
     */
    private function writeChunk(int $partition, int $chunkIndex, array $chunk): string
    {
        // Sort chunk by serialized key
        usort($chunk, fn($a, $b) => strcmp($a['serialized_key'], $b['serialized_key']));

        $chunkFile = "{$this->workingDir}/chunk_{$partition}_{$chunkIndex}.tmp";
        $writer = new BufferedFileWriter($chunkFile, $this->bufferSize);

        try {
            foreach ($chunk as $record) {
                $writer->writeLine(serialize([
                    'serialized_key' => $record['serialized_key'],
                    'original_key' => $record['original_key'],
                    'value' => $record['value']
                ]) . "\n");
            }

            $writer->close();
        } catch (\Exception $e) {
            $writer->close();
            throw $e;
        }

        return $chunkFile;
    }

    /**
     * Merge sorted chunks using k-way merge with value grouping
     *
     * Uses a min-heap for efficient merging, reducing time complexity from O(n*k) to O(n*log k)
     * where n is total records and k is number of chunks.
     *
     * @param int $partition Partition number
     * @param array<int, string> $chunkFiles List of sorted chunk files
     * @return string Path to the final shuffled file
     */
    private function mergeChunks(int $partition, array $chunkFiles): string
    {
        $shuffledFile = "{$this->workingDir}/shuffled_{$partition}.tmp";

        if (empty($chunkFiles)) {
            touch($shuffledFile);
            return $shuffledFile;
        }

        // Open buffered readers for all chunk files (memory-efficient)
        $readers = [];
        $heap = new MergeMinHeap();

        foreach ($chunkFiles as $i => $chunkFile) {
            try {
                // Create buffered reader (reads in 64KB chunks)
                $reader = new BufferedFileReader($chunkFile);
                $readers[$i] = $reader;

                // Add first record from this chunk to heap
                $line = $reader->getLine();
                if ($line !== null) {
                    $heap->insert([
                        'record' => unserialize($line),
                        'fileIndex' => $i
                    ]);
                }
            } catch (\RuntimeException $e) {
                // Close any already opened readers
                foreach ($readers as $r) {
                    $r->close();
                }
                throw new \RuntimeException("Failed to open chunk file: $chunkFile");
            }
        }

        // Create buffered writer for output
        try {
            $writer = new BufferedFileWriter($shuffledFile, $this->bufferSize);
        } catch (\RuntimeException $e) {
            // Close all readers before throwing
            foreach ($readers as $reader) {
                $reader->close();
            }
            throw new \RuntimeException("Failed to create shuffled file: $shuffledFile");
        }

        try {
            $currentKey = null;
            $currentValues = [];
            $currentOriginalKey = null;

            while (!$heap->isEmpty()) {
                // Extract the record with smallest key - O(log k)
                $element = $heap->extract();
                $minRecord = $element['record'];
                $fileIndex = $element['fileIndex'];

                // If this is a new key, write the previous key's group (if any)
                if ($currentKey !== null && $currentKey !== $minRecord['serialized_key']) {
                    $writer->writeLine(serialize([
                        'key' => $currentOriginalKey,
                        'values' => $currentValues
                    ]) . "\n");

                    $currentValues = [];
                }

                // Accumulate values for this key
                $currentKey = $minRecord['serialized_key'];
                $currentOriginalKey = $minRecord['original_key'];
                $currentValues[] = $minRecord['value'];

                // Read next record from the buffered reader
                $line = $readers[$fileIndex]->getLine();
                if ($line !== null) {
                    $heap->insert([
                        'record' => unserialize($line),
                        'fileIndex' => $fileIndex
                    ]);
                }
            }

            // Write the final group
            if ($currentKey !== null) {
                $writer->writeLine(serialize([
                    'key' => $currentOriginalKey,
                    'values' => $currentValues
                ]) . "\n");
            }

            // Close writer and all readers
            $writer->close();
            foreach ($readers as $reader) {
                $reader->close();
            }
        } catch (\Exception $e) {
            // Ensure cleanup on error
            $writer->close();
            foreach ($readers as $reader) {
                $reader->close();
            }
            throw $e;
        }

        return $shuffledFile;
    }

    /**
     * Serialize a key for consistent hashing and sorting
     *
     * @param mixed $key
     * @return string
     */
    private function serializeKey($key): string
    {
        if (is_scalar($key)) {
            return (string)$key;
        }
        return serialize($key);
    }
}
