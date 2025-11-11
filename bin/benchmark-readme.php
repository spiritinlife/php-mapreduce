#!/usr/bin/env php
<?php

declare(strict_types=1);

/**
 * README Benchmark
 *
 * Runs the exact benchmarks shown in the README to verify the numbers.
 * Tests: 10K, 100K, 1M, 10M records
 */

require __DIR__ . '/../vendor/autoload.php';

use Spiritinlife\MapReduce\MapReduceBuilder;

function generateData(int $count): Generator
{
    $categories = range(0, 99);
    for ($i = 0; $i < $count; $i++) {
        yield [
            'category' => 'cat_' . $categories[$i % 100],
            'value' => rand(1, 1000),
            'timestamp' => time() - rand(0, 86400),
        ];
    }
}

function formatBytes(int $bytes): string
{
    return round($bytes / 1024 / 1024) . 'MB';
}

function formatTime(float $seconds): string
{
    if ($seconds < 60) {
        return round($seconds, 1) . 's';
    }
    $minutes = (int)floor($seconds / 60);
    $remainder = $seconds - ($minutes * 60);
    $secs = (int)round($remainder);
    return "{$minutes}m{$secs}s";
}

function runBenchmark(int $recordCount, int $concurrency): array
{
    echo "Testing {$recordCount} records with concurrency={$concurrency}...\n";

    $tempDir = sys_get_temp_dir() . '/benchmark_' . uniqid();
    mkdir($tempDir, 0755, true);

    gc_collect_cycles();
    $memoryBefore = memory_get_usage(true);
    $timeBefore = microtime(true);

    try {
        // Use larger batch sizes for larger datasets to reduce process spawning overhead
        // Fewer, larger batches = less parent process memory overhead
        if ($recordCount >= 1000000) {
            $mapperBatchSize = 5000;
        } elseif ($recordCount >= 100000) {
            $mapperBatchSize = 2000;
        } else {
            $mapperBatchSize = 500;
        }

        $generator = (new MapReduceBuilder())
            ->input(generateData($recordCount))
            ->map(fn($record) => yield [$record['category'], $record['value']])
            ->reduce(fn($category, $values) => [
                'sum' => array_sum($values),
                'count' => count($values),
                'avg' => array_sum($values) / count($values),
            ])
            ->concurrent($concurrency)
            ->partitions($concurrency)
            ->mapperBatchSize($mapperBatchSize)
            ->workingDirectory($tempDir)
            ->execute();

        $result = [];
        foreach ($generator as $key => $data) {
            $result[$key] = $data;
        }

        $timeAfter = microtime(true);
        $memoryPeak = memory_get_peak_usage(true);

        $elapsedTime = $timeAfter - $timeBefore;
        // Note: With parallel processing, child processes use separate memory
        // This shows parent process memory increase only
        $memoryUsed = max(0, $memoryPeak - $memoryBefore);

        return [
            'success' => true,
            'time' => $elapsedTime,
            'memory' => $memoryUsed,
            'results' => count($result),
        ];
    } finally {
        // Cleanup
        if (is_dir($tempDir)) {
            $files = new RecursiveIteratorIterator(
                new RecursiveDirectoryIterator($tempDir, RecursiveDirectoryIterator::SKIP_DOTS),
                RecursiveIteratorIterator::CHILD_FIRST
            );
            foreach ($files as $fileinfo) {
                $fileinfo->isDir() ? rmdir($fileinfo->getRealPath()) : unlink($fileinfo->getRealPath());
            }
            rmdir($tempDir);
        }
    }
}

echo "\n";
echo "========================================\n";
echo "README Benchmark Suite\n";
echo "========================================\n";
echo "\n";

$tests = [
    ['records' => 10000, 'concurrency' => 4],
    ['records' => 100000, 'concurrency' => 4],
    ['records' => 1000000, 'concurrency' => 8],
    ['records' => 10000000, 'concurrency' => 8],
];

$results = [];

foreach ($tests as $test) {
    $result = runBenchmark($test['records'], $test['concurrency']);
    $results[] = [
        'records' => $test['records'],
        'concurrency' => $test['concurrency'],
        'time' => $result['time'],
        'memory' => $result['memory'],
    ];
    echo "  Time: " . formatTime($result['time']) . "\n";
    echo "  Memory: " . formatBytes($result['memory']) . "\n";
    echo "  Result count: {$result['results']}\n";
    echo "\n";
}

echo "========================================\n";
echo "Summary Table\n";
echo "========================================\n\n";

echo "| Records | Concurrency | Time | Memory |\n";
echo "|---------|-------------|------|--------|\n";
foreach ($results as $r) {
    $recordsStr = number_format($r['records']);
    if ($r['records'] >= 1000000) {
        $recordsStr = ($r['records'] / 1000000) . 'M';
    } elseif ($r['records'] >= 1000) {
        $recordsStr = ($r['records'] / 1000) . 'K';
    }

    echo "| " . str_pad($recordsStr, 7) . " | ";
    echo str_pad((string)$r['concurrency'], 11) . " | ";
    echo str_pad(formatTime($r['time']), 4) . " | ";
    echo str_pad(formatBytes($r['memory']), 6) . " |\n";
}

echo "\n";
