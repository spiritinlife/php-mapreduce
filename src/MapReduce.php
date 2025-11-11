<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce;

use Spiritinlife\MapReduce\Pipeline\Mapper;
use Spiritinlife\MapReduce\Pipeline\Shuffler;
use Spiritinlife\MapReduce\Pipeline\Reducer;

/**
 * High-performance MapReduce orchestrator
 *
 * This class orchestrates the three main phases of MapReduce:
 * 1. Map Phase (handled by Mapper)
 * 2. Shuffle Phase (handled by Shuffler)
 * 3. Reduce Phase (handled by Reducer)
 *
 * @package Spiritinlife\MapReduce
 */
class MapReduce
{
    protected string $workingDir;
    protected int $concurrency;
    protected int $shuffleChunkSize;
    protected int $bufferSize;
    protected int $mapperBatchSize;
    protected Mapper $mapper;
    protected Shuffler $shuffler;
    protected Reducer $reducer;

    /**
     * Create a new MapReduce instance
     *
     * Configuration Parameters:
     * - concurrency: Number of parallel worker processes (default: 4)
     *   Higher = more parallelism, but more overhead. Match to CPU cores for best performance.
     *
     * - shuffleChunkSize: Records per chunk during shuffle sort phase (default: 10000)
     *   Controls memory usage during external sorting. Larger = faster sorting, more memory.
     *   Recommended: 5K-50K depending on record size and available memory.
     *
     * - bufferSize: Records buffered in memory before flushing to disk (default: 1000)
     *   Affects I/O performance. Larger = fewer syscalls, more memory per partition.
     *   Memory impact: ~(bufferSize × avg_record_size × num_partitions) bytes.
     *
     * - mapperBatchSize: Items sent to each parallel worker batch (default: 500)
     *   Controls parallelization granularity. Smaller = better load balancing, more overhead.
     *   Larger = less overhead, but potential uneven work distribution.
     *
     * @param int $concurrency Number of concurrent worker processes (default: 4)
     * @param string|null $workingDir Directory for temporary files (default: system temp)
     * @param int $shuffleChunkSize Records per chunk in shuffle external sort (default: 10000)
     * @param int $bufferSize File I/O buffer size in records (default: 1000)
     * @param int $mapperBatchSize Items per mapper worker batch (default: 500)
     */
    public function __construct(
        int $concurrency = 4,
        ?string $workingDir = null,
        int $shuffleChunkSize = 10000,
        int $bufferSize = 1000,
        int $mapperBatchSize = 500
    ) {
        if ($concurrency < 1) {
            throw new \InvalidArgumentException('Concurrency must be at least 1');
        }

        if ($shuffleChunkSize < 1) {
            throw new \InvalidArgumentException('Shuffle chunk size must be at least 1');
        }

        if ($bufferSize < 1) {
            throw new \InvalidArgumentException('Buffer size must be at least 1');
        }

        if ($mapperBatchSize < 1) {
            throw new \InvalidArgumentException('Mapper batch size must be at least 1');
        }

        $this->concurrency = $concurrency;
        $this->shuffleChunkSize = $shuffleChunkSize;
        $this->bufferSize = $bufferSize;
        $this->mapperBatchSize = $mapperBatchSize;
        $this->workingDir = $workingDir ?? sys_get_temp_dir() . '/mapreduce_' . uniqid('mr_', true);

        if (!is_dir($this->workingDir) && !mkdir($this->workingDir, 0755, true)) {
            throw new \RuntimeException("Failed to create working directory: {$this->workingDir}");
        }

        // Initialize the three pipeline components
        $this->mapper = new Mapper($this->workingDir, $this->concurrency, null, $this->bufferSize, $this->mapperBatchSize);
        $this->shuffler = new Shuffler($this->workingDir, $this->shuffleChunkSize, $this->bufferSize, $this->concurrency);
        $this->reducer = new Reducer($this->concurrency, $this->workingDir);
    }

    /**
     * Set a custom partitioner function to control data distribution
     *
     * @param callable $partitioner Function(mixed $key, int $numPartitions): int
     * @return self
     */
    public function withPartitioner(callable $partitioner): self
    {
        $this->mapper = new Mapper($this->workingDir, $this->concurrency, $partitioner, $this->bufferSize, $this->mapperBatchSize);
        return $this;
    }

    /**
     * Execute MapReduce operation
     *
     * @param iterable<mixed, mixed> $input Input data to process
     * @param callable $mapper Function(mixed $key, mixed $value): iterable<array{0: mixed, 1: mixed}>
     * @param callable $reducer Function(mixed $key, array<int, mixed> $values): mixed
     * @param int|null $reducePartitions Number of reduce partitions (default: same as concurrency)
     * @return \Generator<string, array{key: mixed, value: mixed}> Generator yielding final results keyed by reduce keys
     * @throws \RuntimeException If execution fails
     */
    public function execute(
        iterable $input,
        callable $mapper,
        callable $reducer,
        ?int $reducePartitions = null
    ): \Generator {
        $reducePartitions = $reducePartitions ?? $this->concurrency;

        if ($reducePartitions < 1) {
            throw new \InvalidArgumentException('Reduce partitions must be at least 1');
        }

        try {
            // Phase 1: Map - process input and emit intermediate key-value pairs
            $mapOutputFiles = $this->mapper->map($input, $mapper, $reducePartitions);

            // Phase 2: Shuffle - group intermediate data by key
            $shuffledFiles = $this->shuffler->shuffle($mapOutputFiles, $reducePartitions);

            // Phase 3: Reduce - aggregate values for each key
            $results = $this->reducer->reduce($shuffledFiles, $reducer);

            // Yield results from the generator
            yield from $results;
        } finally {
            // Always cleanup temporary files
            $this->cleanup();
        }
    }

    /**
     * Cleanup temporary files
     */
    protected function cleanup(): void
    {
        $this->deleteDirectory($this->workingDir);
    }

    /**
     * Recursively delete a directory and its contents
     *
     * @param string $dir Directory to delete
     */
    protected function deleteDirectory(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }

        $files = @scandir($dir);
        if ($files === false) {
            return;
        }

        $files = array_diff($files, ['.', '..']);

        foreach ($files as $file) {
            $path = "$dir/$file";
            if (is_dir($path)) {
                $this->deleteDirectory($path);
            } else {
                @unlink($path);
            }
        }

        @rmdir($dir);
    }

    /**
     * Get working directory path
     *
     * @return string Working directory path
     */
    public function getWorkingDir(): string
    {
        return $this->workingDir;
    }

    /**
     * Get the Mapper component
     *
     * @return Mapper The mapper instance
     */
    public function getMapper(): Mapper
    {
        return $this->mapper;
    }

    /**
     * Get the Shuffler component
     *
     * @return Shuffler The shuffler instance
     */
    public function getShuffler(): Shuffler
    {
        return $this->shuffler;
    }

    /**
     * Get the Reducer component
     *
     * @return Reducer The reducer instance
     */
    public function getReducer(): Reducer
    {
        return $this->reducer;
    }
}
