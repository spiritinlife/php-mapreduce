#!/usr/bin/env php
<?php

declare(strict_types=1);

/**
 * MapReduce Performance Benchmark
 *
 * This script benchmarks different concurrency and partition configurations
 * to help identify optimal settings for various workload sizes.
 *
 * Usage:
 *   php bin/benchmark.php [test-name]
 *
 * Available tests:
 *   - wordcount: Word count performance matrix
 *   - scaling: Aggregation performance scaling
 *   - partitions: Partition count impact
 *   - concurrency: Concurrency level impact
 *   - memory: Memory efficiency test
 *   - buffer: Buffer size impact test
 *   - comprehensive: Full benchmark suite (default)
 */

require __DIR__ . '/../vendor/autoload.php';

use Spiritinlife\MapReduce\MapReduceBuilder;

class PerformanceBenchmark
{
    private string $tempDir;

    public function __construct()
    {
        $this->tempDir = sys_get_temp_dir() . '/perf_test_' . uniqid();
        mkdir($this->tempDir, 0755, true);
    }

    public function __destruct()
    {
        if (is_dir($this->tempDir)) {
            $this->recursiveDelete($this->tempDir);
        }
    }

    private function recursiveDelete(string $dir): void
    {
        $files = array_diff(scandir($dir), ['.', '..']);
        foreach ($files as $file) {
            $path = "$dir/$file";
            is_dir($path) ? $this->recursiveDelete($path) : unlink($path);
        }
        rmdir($dir);
    }

    private function generateWordCountData(int $documentCount, int $wordsPerDoc = 100): array
    {
        $data = [];
        $words = ['the', 'quick', 'brown', 'fox', 'jumps', 'over', 'lazy', 'dog',
                  'hello', 'world', 'test', 'data', 'performance', 'benchmark'];

        for ($i = 0; $i < $documentCount; $i++) {
            $text = [];
            for ($j = 0; $j < $wordsPerDoc; $j++) {
                $text[] = $words[array_rand($words)];
            }
            $data["doc_$i"] = implode(' ', $text);
        }

        return $data;
    }

    private function generateAggregationData(int $recordCount): array
    {
        $data = [];
        for ($i = 0; $i < $recordCount; $i++) {
            $data[] = [
                'category' => 'cat_' . ($i % 100),
                'value' => rand(1, 1000),
                'timestamp' => time() - rand(0, 86400),
            ];
        }
        return $data;
    }

    private function runTest(
        array $input,
        callable $mapper,
        callable $reducer,
        int $concurrency,
        int $partitions,
        string $testName
    ): array {
        gc_collect_cycles();
        $memoryBefore = memory_get_usage(true);
        $timeBefore = microtime(true);

        $result = (new MapReduceBuilder())
            ->input($input)
            ->map($mapper)
            ->reduce($reducer)
            ->concurrent($concurrency)
            ->partitions($partitions)
            ->workingDirectory($this->tempDir . "/{$testName}_c{$concurrency}_p{$partitions}")
            ->execute();

        $timeAfter = microtime(true);
        $memoryAfter = memory_get_peak_usage(true);

        return [
            'concurrency' => $concurrency,
            'partitions' => $partitions,
            'execution_time' => round(($timeAfter - $timeBefore) * 1000, 2),
            'memory_used' => round(($memoryAfter - $memoryBefore) / 1024 / 1024, 2),
            'result_count' => count($result),
        ];
    }

    private function formatResultsTable(array $results, string $title): string
    {
        $output = "\n=== $title ===\n";
        $output .= str_pad('Concurrency', 12) . ' | ';
        $output .= str_pad('Partitions', 11) . ' | ';
        $output .= str_pad('Time (ms)', 10) . ' | ';
        $output .= str_pad('Memory (MB)', 12) . ' | ';
        $output .= str_pad('Results', 10) . "\n";
        $output .= str_repeat('-', 70) . "\n";

        foreach ($results as $result) {
            $output .= str_pad((string)$result['concurrency'], 12) . ' | ';
            $output .= str_pad((string)$result['partitions'], 11) . ' | ';
            $output .= str_pad(number_format($result['execution_time'], 2), 10) . ' | ';
            $output .= str_pad(number_format($result['memory_used'], 2), 12) . ' | ';
            $output .= str_pad((string)$result['result_count'], 10) . "\n";
        }

        return $output;
    }

    private function findOptimalConfig(array $results): array
    {
        usort($results, fn($a, $b) => $a['execution_time'] <=> $b['execution_time']);
        return $results[0];
    }

    public function wordCountPerformanceMatrix(): void
    {
        $dataSize = 5000;
        $data = $this->generateWordCountData($dataSize, 50);

        echo "\n\n========================================\n";
        echo "Word Count Performance Test\n";
        echo "Dataset: {$dataSize} documents x 50 words\n";
        echo "========================================\n";

        $mapper = function ($text) {
            foreach (str_word_count(strtolower($text), 1) as $word) {
                yield [$word, 1];
            }
        };

        $reducer = fn($word, $counts) => array_sum($counts);

        $configurations = [
            ['concurrency' => 1, 'partitions' => 2],
            ['concurrency' => 2, 'partitions' => 2],
            ['concurrency' => 2, 'partitions' => 4],
            ['concurrency' => 4, 'partitions' => 4],
            ['concurrency' => 4, 'partitions' => 8],
            ['concurrency' => 8, 'partitions' => 8],
            ['concurrency' => 8, 'partitions' => 16],
        ];

        $results = [];
        foreach ($configurations as $config) {
            $result = $this->runTest(
                $data,
                $mapper,
                $reducer,
                $config['concurrency'],
                $config['partitions'],
                'wordcount'
            );
            $results[] = $result;
        }

        echo $this->formatResultsTable($results, 'Configuration Performance');

        $optimal = $this->findOptimalConfig($results);
        echo "\nOptimal Configuration:\n";
        echo "  Concurrency: {$optimal['concurrency']}\n";
        echo "  Partitions: {$optimal['partitions']}\n";
        echo "  Time: {$optimal['execution_time']}ms\n";
        echo "  Memory: {$optimal['memory_used']}MB\n";

        $sequential = $results[0]['execution_time'];
        $bestParallel = $optimal['execution_time'];
        echo "\nSpeedup: " . round($sequential / $bestParallel, 2) . "x\n";
    }

    public function aggregationPerformanceScaling(): void
    {
        echo "\n\n========================================\n";
        echo "Aggregation Performance Scaling Test\n";
        echo "========================================\n";

        $mapper = function ($record) {
            yield [$record['category'], $record['value']];
        };

        $reducer = function ($category, $values) {
            return [
                'sum' => array_sum($values),
                'count' => count($values),
                'avg' => array_sum($values) / count($values),
                'max' => max($values),
                'min' => min($values),
            ];
        };

        $dataSizes = [1000, 5000, 10000, 20000];
        $concurrency = 4;
        $partitions = 8;

        $results = [];
        foreach ($dataSizes as $size) {
            $data = $this->generateAggregationData($size);

            $result = $this->runTest(
                $data,
                $mapper,
                $reducer,
                $concurrency,
                $partitions,
                "agg_$size"
            );
            $result['dataset_size'] = $size;
            $results[] = $result;
        }

        echo "\nScaling Results (Concurrency: $concurrency, Partitions: $partitions):\n";
        echo str_pad('Data Size', 12) . ' | ';
        echo str_pad('Time (ms)', 10) . ' | ';
        echo str_pad('Memory (MB)', 12) . ' | ';
        echo str_pad('Records/sec', 15) . "\n";
        echo str_repeat('-', 60) . "\n";

        foreach ($results as $result) {
            $recordsPerSec = ($result['dataset_size'] / $result['execution_time']) * 1000;
            echo str_pad((string)$result['dataset_size'], 12) . ' | ';
            echo str_pad(number_format($result['execution_time'], 2), 10) . ' | ';
            echo str_pad(number_format($result['memory_used'], 2), 12) . ' | ';
            echo str_pad(number_format($recordsPerSec, 0), 15) . "\n";
        }
    }

    public function partitionCountImpact(): void
    {
        $dataSize = 10000;
        $data = $this->generateAggregationData($dataSize);

        echo "\n\n========================================\n";
        echo "Partition Count Impact Test\n";
        echo "Dataset: {$dataSize} records\n";
        echo "Concurrency: Fixed at 4\n";
        echo "========================================\n";

        $mapper = fn($record) => yield [$record['category'], $record['value']];
        $reducer = fn($category, $values) => ['sum' => array_sum($values), 'count' => count($values)];

        $partitionCounts = [2, 4, 8, 16, 32];
        $results = [];

        foreach ($partitionCounts as $partitions) {
            $result = $this->runTest(
                $data,
                $mapper,
                $reducer,
                4,
                $partitions,
                "partition_test_$partitions"
            );
            $results[] = $result;
        }

        echo $this->formatResultsTable($results, 'Partition Count Performance');

        echo "\nAnalysis:\n";
        $optimal = $this->findOptimalConfig($results);
        echo "- Optimal partition count: {$optimal['partitions']}\n";
        echo "- Best time: {$optimal['execution_time']}ms\n";

        $time2 = $results[0]['execution_time'];
        $time32 = $results[count($results) - 1]['execution_time'];
        $ratio = round($time2 / $time32, 2);

        if ($ratio > 1) {
            echo "- More partitions are faster: {$ratio}x speedup from 2 to 32 partitions\n";
        } else {
            echo "- Fewer partitions are faster: overhead dominates at high partition counts\n";
        }
    }

    public function concurrencyImpact(): void
    {
        $dataSize = 10000;
        $data = $this->generateAggregationData($dataSize);

        echo "\n\n========================================\n";
        echo "Concurrency Impact Test\n";
        echo "Dataset: {$dataSize} records\n";
        echo "Partitions: Fixed at 8\n";
        echo "========================================\n";

        $mapper = fn($record) => yield [$record['category'], $record['value']];
        $reducer = fn($category, $values) => array_sum($values);

        $concurrencyLevels = [1, 2, 4, 8, 16];
        $results = [];

        foreach ($concurrencyLevels as $concurrency) {
            $result = $this->runTest(
                $data,
                $mapper,
                $reducer,
                $concurrency,
                8,
                "concurrency_test_$concurrency"
            );
            $results[] = $result;
        }

        echo $this->formatResultsTable($results, 'Concurrency Level Performance');

        echo "\nSpeedup Analysis (vs sequential):\n";
        $baseline = $results[0]['execution_time'];
        foreach ($results as $result) {
            if ($result['concurrency'] > 1) {
                $speedup = round($baseline / $result['execution_time'], 2);
                echo "  {$result['concurrency']} workers: {$speedup}x speedup\n";
            }
        }
    }

    public function memoryEfficiency(): void
    {
        $dataSize = 10000;
        $data = $this->generateAggregationData($dataSize);

        echo "\n\n========================================\n";
        echo "Memory Efficiency Test\n";
        echo "Dataset: {$dataSize} records\n";
        echo "========================================\n";

        $mapper = fn($record) => yield [$record['category'], $record['value']];
        $reducer = fn($category, $values) => array_sum($values);

        $configurations = [
            ['concurrency' => 1, 'partitions' => 2],
            ['concurrency' => 4, 'partitions' => 4],
            ['concurrency' => 8, 'partitions' => 8],
            ['concurrency' => 16, 'partitions' => 16],
        ];

        $results = [];
        foreach ($configurations as $config) {
            $result = $this->runTest(
                $data,
                $mapper,
                $reducer,
                $config['concurrency'],
                $config['partitions'],
                'memory_test'
            );
            $results[] = $result;
        }

        echo $this->formatResultsTable($results, 'Memory Usage by Configuration');

        usort($results, fn($a, $b) => $a['memory_used'] <=> $b['memory_used']);
        $efficient = $results[0];

        echo "\nMost Memory Efficient:\n";
        echo "  Concurrency: {$efficient['concurrency']}\n";
        echo "  Partitions: {$efficient['partitions']}\n";
        echo "  Memory: {$efficient['memory_used']}MB\n";
        echo "  Time: {$efficient['execution_time']}ms\n";
    }

    public function bufferSizeImpact(): void
    {
        $dataSize = 10000;
        $data = $this->generateAggregationData($dataSize);

        echo "\n\n========================================\n";
        echo "Buffer Size Impact Test\n";
        echo "Dataset: {$dataSize} records\n";
        echo "Fixed: Concurrency=4, Partitions=8\n";
        echo "========================================\n";

        $mapper = fn($record) => yield [$record['category'], $record['value']];
        $reducer = fn($category, $values) => [
            'sum' => array_sum($values),
            'count' => count($values)
        ];

        $bufferSizes = [100, 500, 1000, 2000, 5000, 10000];
        $results = [];

        foreach ($bufferSizes as $bufferSize) {
            gc_collect_cycles();
            $memoryBefore = memory_get_usage(true);
            $timeBefore = microtime(true);

            $result = (new MapReduceBuilder())
                ->input($data)
                ->map($mapper)
                ->reduce($reducer)
                ->concurrent(4)
                ->partitions(8)
                ->bufferSize($bufferSize)
                ->workingDirectory($this->tempDir . "/buffer_test_{$bufferSize}")
                ->execute();

            $timeAfter = microtime(true);
            $memoryAfter = memory_get_peak_usage(true);

            $results[] = [
                'buffer_size' => $bufferSize,
                'execution_time' => round(($timeAfter - $timeBefore) * 1000, 2),
                'memory_used' => round(($memoryAfter - $memoryBefore) / 1024 / 1024, 2),
                'result_count' => count($result),
            ];
        }

        echo "\nBuffer Size Performance:\n";
        echo str_pad('Buffer Size', 12) . ' | ';
        echo str_pad('Time (ms)', 10) . ' | ';
        echo str_pad('Memory (MB)', 12) . ' | ';
        echo str_pad('Efficiency', 12) . "\n";
        echo str_repeat('-', 60) . "\n";

        foreach ($results as $result) {
            $efficiency = $result['execution_time'] > 0
                ? round($result['memory_used'] / $result['execution_time'] * 1000, 2)
                : 0;

            echo str_pad((string)$result['buffer_size'], 12) . ' | ';
            echo str_pad(number_format($result['execution_time'], 2), 10) . ' | ';
            echo str_pad(number_format($result['memory_used'], 2), 12) . ' | ';
            echo str_pad(number_format($efficiency, 2), 12) . "\n";
        }

        // Find optimal buffer size (best time)
        usort($results, fn($a, $b) => $a['execution_time'] <=> $b['execution_time']);
        $optimal = $results[0];

        // Find most memory efficient
        usort($results, fn($a, $b) => $a['memory_used'] <=> $b['memory_used']);
        $memEfficient = $results[0];

        echo "\nOptimal for Performance:\n";
        echo "  Buffer Size: {$optimal['buffer_size']}\n";
        echo "  Time: {$optimal['execution_time']}ms\n";
        echo "  Memory: {$optimal['memory_used']}MB\n";

        echo "\nOptimal for Memory:\n";
        echo "  Buffer Size: {$memEfficient['buffer_size']}\n";
        echo "  Time: {$memEfficient['execution_time']}ms\n";
        echo "  Memory: {$memEfficient['memory_used']}MB\n";

        echo "\nRecommendations:\n";
        echo "  - Small buffers (100-500): Lower memory, more I/O overhead\n";
        echo "  - Medium buffers (1000-2000): Balanced performance (default: 1000)\n";
        echo "  - Large buffers (5000-10000): Best I/O performance, higher memory\n";
        echo "  - Memory = P × bufferSize × avg_record_size (P = partitions)\n";
    }

    public function comprehensiveBenchmark(): void
    {
        echo "\n\n========================================\n";
        echo "COMPREHENSIVE PERFORMANCE BENCHMARK\n";
        echo "========================================\n";

        $testCases = [
            [
                'name' => 'Small Dataset',
                'size' => 1000,
                'concurrency_range' => [1, 2, 4],
                'partition_range' => [2, 4, 8],
            ],
            [
                'name' => 'Medium Dataset',
                'size' => 5000,
                'concurrency_range' => [2, 4, 8],
                'partition_range' => [4, 8, 16],
            ],
            [
                'name' => 'Large Dataset',
                'size' => 10000,
                'concurrency_range' => [4, 8, 16],
                'partition_range' => [8, 16, 32],
            ],
        ];

        $mapper = fn($record) => yield [$record['category'], $record['value']];
        $reducer = fn($category, $values) => [
            'sum' => array_sum($values),
            'count' => count($values)
        ];

        foreach ($testCases as $testCase) {
            echo "\n--- {$testCase['name']}: {$testCase['size']} records ---\n";

            $data = $this->generateAggregationData($testCase['size']);
            $results = [];

            foreach ($testCase['concurrency_range'] as $concurrency) {
                foreach ($testCase['partition_range'] as $partitions) {
                    $result = $this->runTest(
                        $data,
                        $mapper,
                        $reducer,
                        $concurrency,
                        $partitions,
                        "comprehensive_{$testCase['size']}"
                    );
                    $results[] = $result;
                }
            }

            echo $this->formatResultsTable($results, $testCase['name']);

            $optimal = $this->findOptimalConfig($results);
            echo "\nBest Configuration: C{$optimal['concurrency']}/P{$optimal['partitions']} ";
            echo "({$optimal['execution_time']}ms, {$optimal['memory_used']}MB)\n";
        }

        echo "\n========================================\n";
        echo "Benchmark Complete!\n";
        echo "========================================\n";
    }
}

// Main execution
$testName = $argv[1] ?? 'comprehensive';

$benchmark = new PerformanceBenchmark();

switch ($testName) {
    case 'wordcount':
        $benchmark->wordCountPerformanceMatrix();
        break;
    case 'scaling':
        $benchmark->aggregationPerformanceScaling();
        break;
    case 'partitions':
        $benchmark->partitionCountImpact();
        break;
    case 'concurrency':
        $benchmark->concurrencyImpact();
        break;
    case 'memory':
        $benchmark->memoryEfficiency();
        break;
    case 'buffer':
        $benchmark->bufferSizeImpact();
        break;
    case 'comprehensive':
    case 'all':
        $benchmark->wordCountPerformanceMatrix();
        $benchmark->aggregationPerformanceScaling();
        $benchmark->partitionCountImpact();
        $benchmark->concurrencyImpact();
        $benchmark->memoryEfficiency();
        $benchmark->bufferSizeImpact();
        $benchmark->comprehensiveBenchmark();
        break;
    default:
        echo "Unknown test: $testName\n";
        echo "Available tests: wordcount, scaling, partitions, concurrency, memory, buffer, comprehensive\n";
        exit(1);
}
