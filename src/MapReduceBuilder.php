<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce;

/**
 * Fluent builder interface for MapReduce operations
 *
 * Provides a chainable API for configuring and executing MapReduce jobs.
 *
 * @package Spiritinlife\MapReduce
 */
class MapReduceBuilder
{
    /** @var iterable<mixed, mixed>|null */
    protected $input = null;
    /** @var callable|null */
    protected $mapper = null;
    /** @var callable|null */
    protected $reducer = null;
    /** @var callable|null */
    protected $partitioner = null;
    protected int $concurrency = 4;
    protected ?int $reducePartitions = null;
    protected ?string $workingDir = null;
    protected int $shuffleChunkSize = 10000;
    protected int $bufferSize = 1000;
    protected int $mapperBatchSize = 500;
    /** @var mixed */
    protected $context = null;

    /**
     * Set the input data source
     *
     * @param iterable<mixed, mixed> $input Input data (array or any iterable)
     * @return self
     */
    public function input(iterable $input): self
    {
        $this->input = $input;
        return $this;
    }

    /**
     * Set the mapper function
     *
     * The mapper receives (key, value) pairs and should yield [newKey, newValue] pairs.
     *
     * Example:
     * ```php
     * ->map(function($docId, $text) {
     *     foreach (str_word_count($text, 1) as $word) {
     *         yield [$word, 1];
     *     }
     * })
     * ```
     *
     * @param callable $mapper Function(mixed $key, mixed $value): iterable<array{0: mixed, 1: mixed}>
     * @return self
     */
    public function map(callable $mapper): self
    {
        $this->mapper = $mapper;
        return $this;
    }

    /**
     * Set the reducer function
     *
     * The reducer receives a key and all values associated with that key,
     * and should return a single aggregated value.
     *
     * Example:
     * ```php
     * ->reduce(function($word, $counts) {
     *     return array_sum($counts);
     * })
     * ```
     *
     * @param callable $reducer Function(mixed $key, array $values): mixed
     * @return self
     */
    public function reduce(callable $reducer): self
    {
        $this->reducer = $reducer;
        return $this;
    }

    /**
     * Set a custom partitioner to control data distribution
     *
     * The partitioner determines which reduce partition a key goes to.
     * Must return an integer between 0 and (numPartitions - 1).
     *
     * Example:
     * ```php
     * ->partitionBy(function($word, $numPartitions) {
     *     // Partition by first letter
     *     return ord($word[0]) % $numPartitions;
     * })
     * ```
     *
     * @param callable $partitioner Function(mixed $key, int $numPartitions): int
     * @return self
     */
    public function partitionBy(callable $partitioner): self
    {
        $this->partitioner = $partitioner;
        return $this;
    }

    /**
     * Set the number of concurrent map workers
     *
     * @param int $concurrency Number of concurrent workers (default: 4)
     * @return self
     */
    public function concurrent(int $concurrency): self
    {
        if ($concurrency < 1) {
            throw new \InvalidArgumentException('Concurrency must be at least 1');
        }

        $this->concurrency = $concurrency;
        return $this;
    }

    /**
     * Set the number of reduce partitions
     *
     * More partitions = better parallelism but more overhead.
     * Default: same as concurrency.
     *
     * @param int $partitions Number of reduce partitions
     * @return self
     */
    public function partitions(int $partitions): self
    {
        if ($partitions < 1) {
            throw new \InvalidArgumentException('Partitions must be at least 1');
        }

        $this->reducePartitions = $partitions;
        return $this;
    }

    /**
     * Set a custom working directory for temporary files
     *
     * @param string $dir Directory path (will be created if it doesn't exist)
     * @return self
     */
    public function workingDirectory(string $dir): self
    {
        $this->workingDir = $dir;
        return $this;
    }

    /**
     * Set the shuffle chunk size for external sorting during shuffle phase
     *
     * Controls memory usage during the shuffle phase's external sorting.
     * The shuffler reads input files, sorts records in memory chunks, then merges them.
     *
     * Larger chunks = better sorting performance, higher memory usage
     * Smaller chunks = lower memory, more merge overhead
     *
     * Recommended values:
     * - Small datasets (<100K records): 5,000-10,000
     * - Medium datasets (100K-1M): 10,000-50,000 (default: 10,000)
     * - Large datasets (>1M): 50,000-100,000
     * - Memory constrained: 1,000-5,000
     *
     * @param int $size Number of records per shuffle sort chunk (default: 10000)
     * @return self
     */
    public function shuffleChunkSize(int $size): self
    {
        if ($size < 1) {
            throw new \InvalidArgumentException('Shuffle chunk size must be at least 1');
        }

        $this->shuffleChunkSize = $size;
        return $this;
    }

    /**
     * DEPRECATED: Use shuffleChunkSize() instead
     * @deprecated Use shuffleChunkSize() for clarity
     */
    public function chunkSize(int $size): self
    {
        return $this->shuffleChunkSize($size);
    }

    /**
     * Set the mapper batch size for parallel processing
     *
     * Controls how many input items are sent to each parallel worker process.
     * This affects parallelization granularity and overhead.
     *
     * Smaller batches = better load balancing, more process overhead
     * Larger batches = less overhead, potential uneven distribution
     *
     * Recommended values:
     * - Small datasets (<10K records): 100-500 (default: 500)
     * - Medium datasets (10K-1M): 500-2,000
     * - Large datasets (>1M): 1,000-5,000
     * - Many small records: 1,000+
     * - Large complex records: 100-500
     *
     * @param int $size Items per parallel worker batch (default: 500)
     * @return self
     */
    public function mapperBatchSize(int $size): self
    {
        if ($size < 1) {
            throw new \InvalidArgumentException('Mapper batch size must be at least 1');
        }

        $this->mapperBatchSize = $size;
        return $this;
    }

    /**
     * Set the I/O buffer size for write operations
     *
     * Controls memory vs I/O performance tradeoff. Records are accumulated in memory
     * until this threshold is reached, then flushed to disk in a single write operation.
     *
     * Memory Impact: Each partition holds up to (bufferSize × avg_record_size) bytes.
     * With P partitions: Total buffer memory ≈ P × bufferSize × avg_record_size
     *
     * Performance Impact:
     * - Larger buffers = Fewer syscalls = Better I/O performance
     * - Smaller buffers = Lower memory usage = More syscalls
     *
     * Recommendations:
     * - Small datasets (<10K records): 100-500
     * - Medium datasets (10K-1M): 1000-5000 (default: 1000)
     * - Large datasets (>1M): 5000-10000
     * - Memory constrained: 100-500
     *
     * @param int $size Number of records to buffer before flushing (default: 1000)
     * @return self
     */
    public function bufferSize(int $size): self
    {
        if ($size < 1) {
            throw new \InvalidArgumentException('Buffer size must be at least 1');
        }

        $this->bufferSize = $size;
        return $this;
    }

    /**
     * Set context data to be passed to mapper and reducer functions
     *
     * Context data is serialized and made available to all mapper and reducer
     * functions executing in parallel worker processes. This allows you to pass
     * configuration, lookup tables, or other shared data without using closures.
     *
     * The context will be passed as the last parameter to your mapper and reducer functions:
     * - Mapper: function($value, $context)
     * - Reducer: function($key, $values, $context)
     *
     * Example:
     * ```php
     * ->context(['threshold' => 5, 'lookup' => ['a' => 1, 'b' => 2]])
     * ->map(function($value, $context) {
     *     if ($value > $context['threshold']) {
     *         yield [$value, 1];
     *     }
     * })
     * ->reduce(function($key, $values, $context) {
     *     return array_sum($values) * $context['lookup'][$key];
     * })
     * ```
     *
     * @param mixed $context Any serializable data (arrays, objects, scalars)
     * @return self
     */
    public function context($context): self
    {
        $this->context = $context;
        return $this;
    }

    /**
     * Execute the MapReduce job
     *
     * @return \Generator<string, array{key: mixed, value: mixed}> Generator yielding final results keyed by reduce keys
     * @throws \RuntimeException If required parameters are missing or execution fails
     */
    public function execute(): \Generator
    {
        if ($this->input === null) {
            throw new \RuntimeException('Input data is required. Use ->input($data)');
        }

        if ($this->mapper === null) {
            throw new \RuntimeException('Mapper function is required. Use ->map($callable)');
        }

        if ($this->reducer === null) {
            throw new \RuntimeException('Reducer function is required. Use ->reduce($callable)');
        }

        $mapReduce = new MapReduce(
            $this->concurrency,
            $this->workingDir,
            $this->shuffleChunkSize,
            $this->bufferSize,
            $this->mapperBatchSize
        );

        if ($this->partitioner !== null) {
            $mapReduce->withPartitioner($this->partitioner);
        }

        yield from $mapReduce->execute(
            $this->input,
            $this->mapper,
            $this->reducer,
            $this->reducePartitions,
            $this->context
        );
    }
}
