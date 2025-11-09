<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Pipeline;

use Spiritinlife\MapReduce\Utils\BufferedFileReader;

use function Amp\async;
use function Amp\Future\await;

/**
 * Reducer component for MapReduce pipeline
 *
 * Handles the reduce phase: processes shuffled data and aggregates
 * values for each key using the reducer function.
 *
 * @package Spiritinlife\MapReduce
 */
class Reducer
{
    /**
     * Execute the reduce phase
     *
     * @param array<int, string> $shuffledFiles List of shuffled files to process
     * @param callable $reducer Reducer function
     * @return array<string, array{key: mixed, value: mixed}> Final results
     */
    public function reduce(array $shuffledFiles, callable $reducer): array
    {
        $futures = [];

        foreach ($shuffledFiles as $filename) {
            $futures[] = async(function () use ($filename, $reducer) {
                return $this->processPartition($filename, $reducer);
            });
        }

        $results = await($futures);

        // Merge all partition results
        $merged = [];
        foreach ($results as $partition) {
            $merged = array_merge($merged, $partition);
        }

        return $merged;
    }

    /**
     * Process a single reduce partition
     *
     * @param string $filename Shuffled file to process
     * @param callable $reducer Reducer function
     * @return array<string, array{key: mixed, value: mixed}> Results for this partition
     */
    protected function processPartition(string $filename, callable $reducer): array
    {
        $results = [];

        if (!file_exists($filename)) {
            return $results;
        }

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

                // Store result
                $results[$this->serializeKey($key)] = [
                    'key' => $key,
                    'value' => $result
                ];
            }

            $reader->close();
        } catch (\Exception $e) {
            $reader->close();
            throw $e;
        }

        return $results;
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
