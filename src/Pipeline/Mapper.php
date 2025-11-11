<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Pipeline;

use Spatie\Async\Pool;
use Spiritinlife\MapReduce\Utils\BufferedFileWriter;

/**
 * Mapper component for MapReduce pipeline
 *
 * Handles the map phase: processes input data in parallel using true
 * process-based concurrency and writes intermediate results to partitioned files.
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
    protected int $mapperBatchSize;
    protected ?string $autoloadPath;

    /**
     * Create a new Mapper instance
     *
     * @param string $workingDir Directory for temporary files
     * @param int $concurrency Number of concurrent worker processes
     * @param callable|null $partitioner Custom partitioner function
     * @param int $bufferSize File I/O buffer size - records buffered before disk flush (default: 1000)
     * @param int $mapperBatchSize Items per worker batch - controls parallelization granularity (default: 500)
     * @param string|null $autoloadPath Path to autoload file for worker processes (e.g., vendor/autoload.php)
     */
    public function __construct(
        string $workingDir,
        int $concurrency = 4,
        ?callable $partitioner = null,
        int $bufferSize = 1000,
        int $mapperBatchSize = 500,
        ?string $autoloadPath = null
    ) {
        $this->workingDir = $workingDir;
        $this->concurrency = $concurrency;
        $this->partitioner = $partitioner;
        $this->bufferSize = $bufferSize;
        $this->mapperBatchSize = $mapperBatchSize;
        $this->autoloadPath = $autoloadPath;
    }

    /**
     * Execute the map phase
     *
     * @param iterable<mixed, mixed> $input Input data to process
     * @param callable $mapper Mapper function
     * @param int $reducePartitions Number of reduce partitions
     * @param mixed $context Optional context data passed to mapper function
     * @return array<int, array<int, string>> Map output files grouped by partition
     */
    public function map(iterable $input, callable $mapper, int $reducePartitions, $context = null): array
    {
        // Create pool with specified concurrency
        $pool = Pool::create()->concurrency($this->concurrency);

        // Configure autoload if provided
        if ($this->autoloadPath !== null) {
            $pool->autoload($this->autoloadPath);
        }

        $chunk = [];
        $chunkIndex = 0;

        // Capture these in local variables for serialization
        $workingDir = $this->workingDir;
        $bufferSize = $this->bufferSize;
        $partitioner = $this->partitioner;

        // Stream chunks to workers as they arrive
        foreach ($input as $item) {
            $chunk[] = $item;

            if (count($chunk) === $this->mapperBatchSize) {
                // Create a copy of chunk for the closure
                $chunkCopy = $chunk;
                $currentChunkIndex = $chunkIndex;

                $pool->add(function () use ($chunkCopy, $currentChunkIndex, $mapper, $reducePartitions, $workingDir, $bufferSize, $partitioner, $context) {
                    return self::processChunk($chunkCopy, $currentChunkIndex, $mapper, $reducePartitions, $workingDir, $bufferSize, $partitioner, $context);
                });

                $chunk = [];
                $chunkIndex++;
            }
        }

        // Process remaining items
        if (!empty($chunk)) {
            $chunkCopy = $chunk;
            $currentChunkIndex = $chunkIndex;

            $pool->add(function () use ($chunkCopy, $currentChunkIndex, $mapper, $reducePartitions, $workingDir, $bufferSize, $partitioner, $context) {
                return self::processChunk($chunkCopy, $currentChunkIndex, $mapper, $reducePartitions, $workingDir, $bufferSize, $partitioner, $context);
            });
        }

        // Wait for all tasks to complete and collect results
        $workerResults = $pool->wait();

        // Aggregate results by partition
        $mapOutputFiles = array_fill(0, $reducePartitions, []);
        foreach ($workerResults as $workerFiles) {
            foreach ($workerFiles as $partition => $file) {
                $mapOutputFiles[$partition][] = $file;
            }
        }

        return $mapOutputFiles;
    }

    /**
     * Process a chunk of items in parallel
     *
     * Static method to enable clean serialization for parallel worker processes.
     *
     * @param array<mixed> $chunk Chunk of items to process
     * @param int $chunkIndex Chunk index for unique file naming
     * @param callable $mapper Mapper function
     * @param int $reducePartitions Number of reduce partitions
     * @param string $workingDir Working directory
     * @param int $bufferSize Buffer size for file writes
     * @param callable|null $partitioner Custom partitioner function
     * @param mixed $context Optional context data passed to mapper function
     * @return array<int, string> Partition files created by this chunk
     */
    protected static function processChunk(
        array $chunk,
        int $chunkIndex,
        callable $mapper,
        int $reducePartitions,
        string $workingDir,
        int $bufferSize,
        ?callable $partitioner = null,
        $context = null
    ): array {
        $partitionWriters = [];
        $partitionFiles = [];

        try {
            // Initialize writers for all partitions
            for ($i = 0; $i < $reducePartitions; $i++) {
                $filename = "{$workingDir}/map_{$chunkIndex}_partition_{$i}.tmp";
                $partitionWriters[$i] = new BufferedFileWriter($filename, $bufferSize);
                $partitionFiles[$i] = $filename;
            }

            // Process each item in the chunk
            foreach ($chunk as $value) {
                // Call mapper with context as the second parameter
                $intermediateResults = $context !== null ? $mapper($value, $context) : $mapper($value);

                if ($intermediateResults === null) {
                    continue;
                }

                if (!is_iterable($intermediateResults)) {
                    $intermediateResults = [$intermediateResults];
                }

                foreach ($intermediateResults as $pair) {
                    if (!is_array($pair) || count($pair) !== 2) {
                        throw new \RuntimeException('Mapper must yield [key, value] pairs');
                    }

                    [$intermediateKey, $intermediateValue] = $pair;
                    $partition = self::getPartition($intermediateKey, $reducePartitions, $partitioner);

                    $partitionWriters[$partition]->writeLine(serialize([
                        'key' => $intermediateKey,
                        'value' => $intermediateValue
                    ]) . "\n");
                }
            }

            // Close all writers
            foreach ($partitionWriters as $writer) {
                $writer->close();
            }
        } catch (\Exception $e) {
            // Ensure cleanup on error
            foreach ($partitionWriters as $writer) {
                $writer->close();
            }
            throw $e;
        }

        return $partitionFiles;
    }

    /**
     * Determine partition for a key
     *
     * Static method to enable use in parallel worker processes.
     *
     * @param mixed $key Key to partition
     * @param int $numPartitions Number of partitions
     * @param callable|null $partitioner Custom partitioner function
     * @return int Partition index (0 to numPartitions-1)
     */
    protected static function getPartition($key, int $numPartitions, ?callable $partitioner = null): int
    {
        if ($partitioner) {
            $partition = $partitioner($key, $numPartitions);

            if (!is_int($partition) || $partition < 0 || $partition >= $numPartitions) {
                throw new \RuntimeException(
                    "Partitioner must return an integer between 0 and " . ($numPartitions - 1)
                );
            }

            return $partition;
        }

        $hash = crc32(self::serializeKey($key));
        return abs($hash) % $numPartitions;
    }

    /**
     * Serialize a key for consistent hashing
     *
     * Static method to enable use in parallel worker processes.
     *
     * @param mixed $key Key to serialize
     * @return string Serialized key
     */
    protected static function serializeKey($key): string
    {
        if (is_scalar($key)) {
            return (string)$key;
        }
        return serialize($key);
    }

}
