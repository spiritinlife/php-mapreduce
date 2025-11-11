<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Pipeline;

use Spatie\Async\Pool;
use Spiritinlife\MapReduce\Utils\BufferedFileReader;
use Spiritinlife\MapReduce\Utils\BufferedFileWriter;

/**
 * Reducer component for MapReduce pipeline
 *
 * Handles the reduce phase: processes shuffled data in parallel
 * and aggregates values for each key using the reducer function.
 *
 * @package Spiritinlife\MapReduce
 */
class Reducer
{
    protected int $concurrency;
    protected string $workingDir;
    protected ?string $autoloadPath;

    /**
     * Create a new Reducer instance
     *
     * @param int $concurrency Number of concurrent workers
     * @param string|null $workingDir Directory for temporary files
     * @param string|null $autoloadPath Path to autoload file for worker processes (e.g., vendor/autoload.php)
     */
    public function __construct(int $concurrency = 4, ?string $workingDir = null, ?string $autoloadPath = null)
    {
        $this->concurrency = $concurrency;
        $this->workingDir = $workingDir ?? sys_get_temp_dir();
        $this->autoloadPath = $autoloadPath;
    }

    /**
     * Execute the reduce phase
     *
     * @param array<int, string> $shuffledFiles List of shuffled files to process
     * @param callable $reducer Reducer function
     * @param mixed $context Optional context data passed to reducer function
     * @return \Generator<string, array{key: mixed, value: mixed}> Generator yielding final results
     */
    public function reduce(array $shuffledFiles, callable $reducer, $context = null): \Generator
    {
        $pool = Pool::create()->concurrency($this->concurrency);

        // Configure autoload if provided
        if ($this->autoloadPath !== null) {
            $pool->autoload($this->autoloadPath);
        }

        foreach ($shuffledFiles as $index => $filename) {
            $pool->add(function () use ($index, $filename, $reducer, $context) {
                return [
                    'index' => $index,
                    'resultFile' => self::processPartition($filename, $reducer, $index, $this->workingDir, $context)
                ];
            });
        }

        $results = $pool->wait();

        // Sort by index to maintain order
        usort($results, fn($a, $b) => $a['index'] <=> $b['index']);

        // Read and yield results from result files
        foreach ($results as $result) {
            $resultFile = $result['resultFile'];

            if (!file_exists($resultFile)) {
                continue;
            }

            $reader = new BufferedFileReader($resultFile);

            try {
                while (($line = $reader->getLine()) !== null) {
                    if (empty($line)) {
                        continue;
                    }

                    $data = unserialize($line);
                    yield $data['serialized_key'] => $data;
                }

                $reader->close();
            } catch (\Exception $e) {
                $reader->close();
                throw $e;
            } finally {
                @unlink($resultFile);
            }
        }
    }

    /**
     * Process a single reduce partition
     *
     * Static method to enable clean serialization for parallel worker processes.
     *
     * @param string $filename Shuffled file to process
     * @param callable $reducer Reducer function with signature: ($key, $valuesIterator, $context)
     * @param int $index Partition index for unique file naming
     * @param string $workingDir Working directory
     * @param mixed $context Optional context data passed to reducer function
     * @return string Path to result file
     */
    protected static function processPartition(string $filename, callable $reducer, int $index, string $workingDir, $context = null): string
    {
        $resultFile = "{$workingDir}/reduce_result_{$index}.tmp";

        if (!file_exists($filename)) {
            touch($resultFile);
            return $resultFile;
        }

        $writer = new BufferedFileWriter($resultFile);

        try {
            // Use buffered reading for memory-safe processing
            $reader = new BufferedFileReader($filename);

            try {
                $currentRecord = null;

                // Read first record
                $line = $reader->getLine();
                if ($line !== null && !empty($line)) {
                    $currentRecord = unserialize($line);
                }

                while ($currentRecord !== null) {
                    $key = $currentRecord['key'];
                    $serializedKey = self::serializeKey($key);

                    // Create an iterator that yields values for this key
                    $valuesIterator = self::createValueIterator($reader, $currentRecord, $serializedKey, $currentRecord);

                    // Call reducer function with iterator and context
                    $result = $context !== null
                        ? $reducer($key, $valuesIterator, $context)
                        : $reducer($key, $valuesIterator);

                    // Write result to file
                    $writer->writeLine(serialize([
                        'key' => $key,
                        'value' => $result,
                        'serialized_key' => $serializedKey
                    ]) . "\n");
                }

                $reader->close();
                $writer->close();
            } catch (\Exception $e) {
                $reader->close();
                $writer->close();
                throw $e;
            }
        } catch (\Exception $e) {
            $writer->close();
            throw $e;
        }

        return $resultFile;
    }

    /**
     * Create an iterator that yields values for consecutive records with the same key
     *
     * This generator function yields values one at a time for memory efficiency,
     * following the Hadoop-style iterator pattern.
     *
     * @param BufferedFileReader $reader File reader positioned after the first record
     * @param array{key: mixed, value: mixed} $firstRecord First record for this key
     * @param string $serializedKey Serialized key to match against
     * @param array{key: mixed, value: mixed}|null &$nextRecord Output parameter - will contain the next record with different key (or null)
     * @return \Generator<int, mixed> Generator yielding values for this key
     */
    private static function createValueIterator(BufferedFileReader $reader, array $firstRecord, string $serializedKey, ?array &$nextRecord): \Generator
    {
        // Yield the first value
        yield $firstRecord['value'];

        // Continue reading while we have the same key
        while (($line = $reader->getLine()) !== null) {
            if (empty($line)) {
                continue;
            }

            $record = unserialize($line);
            $recordSerializedKey = self::serializeKey($record['key']);

            if ($recordSerializedKey === $serializedKey) {
                // Same key - yield the value
                yield $record['value'];
            } else {
                // Different key - save it for the next iteration and stop
                $nextRecord = $record;
                return;
            }
        }

        // No more records - set nextRecord to null
        $nextRecord = null;
    }

    /**
     * Serialize a key for consistent grouping
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
