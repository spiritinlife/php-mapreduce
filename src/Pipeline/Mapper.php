<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Pipeline;

use Spiritinlife\MapReduce\Utils\BufferedFileWriter;

use function Amp\async;
use function Amp\Future\await;

/**
 * Mapper component for MapReduce pipeline
 *
 * Handles the map phase: processes input data concurrently and writes
 * intermediate results to partitioned files.
 *
 * @package Spiritinlife\MapReduce
 */
class Mapper
{
    protected string $workingDir;
    protected int $concurrency;
    /** @var callable|null */
    protected $partitioner = null;
    protected int $bufferSize;

    /**
     * Create a new Mapper instance
     *
     * @param string $workingDir Directory for temporary files
     * @param int $concurrency Number of concurrent workers
     * @param callable|null $partitioner Custom partitioner function
     * @param int $bufferSize Number of records to buffer before flushing (default: 1000)
     */
    public function __construct(
        string $workingDir,
        int $concurrency = 4,
        ?callable $partitioner = null,
        int $bufferSize = 1000
    ) {
        $this->workingDir = $workingDir;
        $this->concurrency = $concurrency;
        $this->partitioner = $partitioner;
        $this->bufferSize = $bufferSize;
    }

    /**
     * Execute the map phase
     *
     * @param iterable<mixed, mixed> $input Input data to process
     * @param callable $mapper Mapper function
     * @param int $reducePartitions Number of reduce partitions
     * @return array<int, array<int, string>> Map output files grouped by partition
     */
    public function map(iterable $input, callable $mapper, int $reducePartitions): array
    {
        $chunks = $this->chunkInput($input, $this->concurrency);

        if (empty($chunks)) {
            return array_fill(0, $reducePartitions, []);
        }

        // Create async tasks for each chunk
        $futures = [];
        foreach ($chunks as $chunkIndex => $chunk) {
            $futures[] = async(function () use ($chunkIndex, $chunk, $mapper, $reducePartitions) {
                return $this->processChunk($chunkIndex, $chunk, $mapper, $reducePartitions);
            });
        }

        // Wait for all mappers to complete
        $results = await($futures);

        // Reorganize by partition: partition -> [file1, file2, ...]
        $mapOutputFiles = array_fill(0, $reducePartitions, []);
        foreach ($results as $mapperFiles) {
            foreach ($mapperFiles as $partition => $file) {
                $mapOutputFiles[$partition][] = $file;
            }
        }

        return $mapOutputFiles;
    }

    /**
     * Process a single chunk in the map phase
     *
     * @param int $chunkIndex Chunk index
     * @param array<mixed, mixed> $chunk Data chunk to process
     * @param callable $mapper Mapper function
     * @param int $reducePartitions Number of reduce partitions
     * @return array<int, string> Partition files created by this mapper
     */
    protected function processChunk(
        int $chunkIndex,
        array $chunk,
        callable $mapper,
        int $reducePartitions
    ): array {
        // Create buffered writers for each partition
        $partitionWriters = [];
        $partitionFiles = [];

        try {
            for ($i = 0; $i < $reducePartitions; $i++) {
                $filename = "{$this->workingDir}/map_{$chunkIndex}_partition_{$i}.tmp";
                $partitionWriters[$i] = new BufferedFileWriter($filename, $this->bufferSize);
                $partitionFiles[$i] = $filename;
            }

            // Process each item in the chunk
            foreach ($chunk as $key => $value) {
                // Call mapper function - it should yield key-value pairs
                $intermediateResults = $mapper($key, $value);

                // Ensure we have an iterable
                if (!is_iterable($intermediateResults)) {
                    $intermediateResults = [$intermediateResults];
                }

                // Write intermediate results to appropriate partition files
                foreach ($intermediateResults as $pair) {
                    if (!is_array($pair) || count($pair) !== 2) {
                        throw new \RuntimeException('Mapper must yield [key, value] pairs');
                    }

                    [$intermediateKey, $intermediateValue] = $pair;

                    // Determine which partition this key belongs to
                    $partition = $this->getPartition($intermediateKey, $reducePartitions);

                    // Write to buffered writer (auto-flushes when buffer is full)
                    $partitionWriters[$partition]->writeLine(serialize([
                        'key' => $intermediateKey,
                        'value' => $intermediateValue
                    ]) . "\n");
                }
            }

            // Close all writers (automatically flushes remaining data)
            foreach ($partitionWriters as $writer) {
                $writer->close();
            }
        } catch (\Exception $e) {
            // Ensure all writers are closed on error
            foreach ($partitionWriters as $writer) {
                $writer->close();
            }
            throw $e;
        }

        return $partitionFiles;
    }

    /**
     * Chunk input data for concurrent processing
     *
     * @param iterable<mixed, mixed> $input Input data
     * @param int $chunks Number of chunks
     * @return array<int, array<mixed, mixed>> Chunked input data
     */
    protected function chunkInput(iterable $input, int $chunks): array
    {
        $array = is_array($input) ? $input : iterator_to_array($input);

        if (empty($array)) {
            return [];
        }

        $chunkSize = max(1, (int)ceil(count($array) / $chunks));
        return array_chunk($array, $chunkSize, true);
    }

    /**
     * Determine partition for a key
     *
     * @param mixed $key Key to partition
     * @param int $numPartitions Number of partitions
     * @return int Partition index (0 to numPartitions-1)
     */
    protected function getPartition($key, int $numPartitions): int
    {
        if ($this->partitioner) {
            $partition = ($this->partitioner)($key, $numPartitions);

            if (!is_int($partition) || $partition < 0 || $partition >= $numPartitions) {
                throw new \RuntimeException(
                    "Partitioner must return an integer between 0 and " . ($numPartitions - 1)
                );
            }

            return $partition;
        }

        // Default: hash-based partitioning
        $hash = crc32($this->serializeKey($key));
        return abs($hash) % $numPartitions;
    }

    /**
     * Serialize a key for consistent hashing
     *
     * @param mixed $key Key to serialize
     * @return string Serialized key
     */
    protected function serializeKey($key): string
    {
        if (is_scalar($key)) {
            return (string)$key;
        }
        return serialize($key);
    }
}
