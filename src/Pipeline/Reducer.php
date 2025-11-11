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

    /**
     * Create a new Reducer instance
     *
     * @param int $concurrency Number of concurrent workers
     * @param string|null $workingDir Directory for temporary files
     */
    public function __construct(int $concurrency = 4, ?string $workingDir = null)
    {
        $this->concurrency = $concurrency;
        $this->workingDir = $workingDir ?? sys_get_temp_dir();
    }

    /**
     * Execute the reduce phase
     *
     * @param array<int, string> $shuffledFiles List of shuffled files to process
     * @param callable $reducer Reducer function
     * @return \Generator<string, array{key: mixed, value: mixed}> Generator yielding final results
     */
    public function reduce(array $shuffledFiles, callable $reducer): \Generator
    {
        $pool = Pool::create()->concurrency($this->concurrency);

        foreach ($shuffledFiles as $index => $filename) {
            $pool->add(function () use ($index, $filename, $reducer) {
                return [
                    'index' => $index,
                    'resultFile' => $this->processPartition($filename, $reducer, $index)
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
     * @param string $filename Shuffled file to process
     * @param callable $reducer Reducer function
     * @param int $index Partition index for unique file naming
     * @return string Path to result file
     */
    protected function processPartition(string $filename, callable $reducer, int $index): string
    {
        $resultFile = "{$this->workingDir}/reduce_result_{$index}.tmp";

        if (!file_exists($filename)) {
            touch($resultFile);
            return $resultFile;
        }

        $writer = new BufferedFileWriter($resultFile);

        try {
            // Use buffered reading for memory-safe processing
            $reader = new BufferedFileReader($filename);

            try {
                while (($line = $reader->getLine()) !== null) {
                    // Skip empty lines
                    if (empty($line)) {
                        continue;
                    }

                    $group = unserialize($line);
                    $key = $group['key'];
                    $values = $group['values'];

                    // Call reducer function
                    $result = $reducer($key, $values);

                    // Write result to file
                    $writer->writeLine(serialize([
                        'key' => $key,
                        'value' => $result,
                        'serialized_key' => $this->serializeKey($key)
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
     * Serialize a key for consistent grouping
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
