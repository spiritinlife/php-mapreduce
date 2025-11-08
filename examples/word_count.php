<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Spiritinlife\MapReduce\MapReduceBuilder;

// Word count example
$documents = [
    'doc1' => 'the quick brown fox jumps over the lazy dog',
    'doc2' => 'the lazy cat sleeps on the warm mat',
    'doc3' => 'quick brown foxes are clever animals that jump',
    'doc4' => 'lazy dogs and cats are sleeping animals',
];

echo "=== Word Count Example ===\n";
echo "Processing " . count($documents) . " documents...\n\n";

$startTime = microtime(true);

$wordCounts = (new MapReduceBuilder())
    ->input($documents)
    ->map(function ($docId, $text) {
        // Split text into words and emit each with count of 1
        $words = str_word_count(strtolower($text), 1);
        foreach ($words as $word) {
            yield [$word, 1];
        }
    })
    ->reduce(function ($word, $counts) {
        // Sum all counts for this word
        return array_sum($counts);
    })
    ->concurrent(2)  // Use 2 concurrent workers
    ->execute();

$duration = microtime(true) - $startTime;

// Sort results by count (descending)
$sorted = [];
foreach ($wordCounts as $data) {
    $sorted[$data['key']] = $data['value'];
}
arsort($sorted);

echo "Results (sorted by frequency):\n";
echo "------------------------------\n";
foreach ($sorted as $word => $count) {
    echo sprintf("%-12s: %d\n", $word, $count);
}

echo "\nExecution time: " . number_format($duration, 3) . " seconds\n";
echo "Total unique words: " . count($wordCounts) . "\n";
