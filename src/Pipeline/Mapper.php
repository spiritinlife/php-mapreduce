<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Pipeline;

use Amp\Pipeline\Queue;
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
        // Queue enables concurrent work distribution via iterate()
        // Multiple workers can safely consume from the same iterator (work-stealing pattern)
        $queue = new Queue();
        $iterator = $queue->iterate();

        // Producer coroutine: push() provides backpressure - blocks until item is consumed
        // This prevents loading entire iterator into memory
        async(function () use ($input, $queue) {
            foreach ($input as $value) {
                $queue->push($value);
            }
            $queue->complete();
        });

        // Spawn concurrent worker coroutines that share the iterator
        // Each worker pulls unique items - no duplication across workers
        $futures = [];
        for ($workerIndex = 0; $workerIndex < $this->concurrency; $workerIndex++) {
            $futures[] = async(function () use ($iterator, $workerIndex, $mapper, $reducePartitions) {
                return $this->processWorker($iterator, $workerIndex, $mapper, $reducePartitions);
            });
        }

        $workerResults = await($futures);

        $mapOutputFiles = array_fill(0, $reducePartitions, []);
        foreach ($workerResults as $workerFiles) {
            foreach ($workerFiles as $partition => $file) {
                $mapOutputFiles[$partition][] = $file;
            }
        }

        return $mapOutputFiles;
    }

    /**
     * Process items from iterator with persistent file writers per worker
     *
     * @param iterable<mixed> $iterator Input iterator to consume from (shared across workers)
     * @param int $workerIndex Worker index for unique file naming
     * @param callable $mapper Mapper function
     * @param int $reducePartitions Number of reduce partitions
     * @return array<int, string> Partition files created by this worker
     */
    protected function processWorker(
        iterable $iterator,
        int $workerIndex,
        callable $mapper,
        int $reducePartitions
    ): array {
        $partitionWriters = [];
        $partitionFiles = [];
        $writersCreated = false;

        try {
            foreach ($iterator as $value) {
                // Lazy initialization: only create files if this worker gets items
                // Empty input = no files created, avoiding unnecessary I/O
                if (!$writersCreated) {
                    for ($i = 0; $i < $reducePartitions; $i++) {
                        $filename = "{$this->workingDir}/map_{$workerIndex}_partition_{$i}.tmp";
                        $partitionWriters[$i] = new BufferedFileWriter($filename, $this->bufferSize);
                        $partitionFiles[$i] = $filename;
                    }
                    $writersCreated = true;
                }

                $intermediateResults = $mapper($value);

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
                    $partition = $this->getPartition($intermediateKey, $reducePartitions);

                    $partitionWriters[$partition]->writeLine(serialize([
                        'key' => $intermediateKey,
                        'value' => $intermediateValue
                    ]) . "\n");
                }
            }

            foreach ($partitionWriters as $writer) {
                $writer->close();
            }
        } catch (\Exception $e) {
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
