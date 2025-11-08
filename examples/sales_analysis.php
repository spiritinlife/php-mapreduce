<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Spiritinlife\MapReduce\MapReduceBuilder;

// Sales analysis example
$sales = [
    ['product' => 'Widget A', 'amount' => 100, 'region' => 'North', 'date' => '2024-01-01'],
    ['product' => 'Gadget B', 'amount' => 150, 'region' => 'South', 'date' => '2024-01-01'],
    ['product' => 'Widget A', 'amount' => 200, 'region' => 'North', 'date' => '2024-01-02'],
    ['product' => 'Widget A', 'amount' => 120, 'region' => 'East', 'date' => '2024-01-02'],
    ['product' => 'Gadget B', 'amount' => 180, 'region' => 'South', 'date' => '2024-01-03'],
    ['product' => 'Tool C', 'amount' => 80, 'region' => 'West', 'date' => '2024-01-03'],
    ['product' => 'Tool C', 'amount' => 90, 'region' => 'North', 'date' => '2024-01-04'],
    ['product' => 'Gadget B', 'amount' => 220, 'region' => 'East', 'date' => '2024-01-04'],
];

echo "=== Sales Analysis Example ===\n";
echo "Processing " . count($sales) . " sales records...\n\n";

$startTime = microtime(true);

// Analyze sales by product
$salesByProduct = (new MapReduceBuilder())
    ->input($sales)
    ->map(function ($id, $sale) {
        yield [$sale['product'], $sale['amount']];
    })
    ->reduce(function ($product, $amounts) {
        return [
            'total' => array_sum($amounts),
            'count' => count($amounts),
            'average' => array_sum($amounts) / count($amounts),
            'min' => min($amounts),
            'max' => max($amounts),
        ];
    })
    ->concurrent(4)
    ->execute();

$duration = microtime(true) - $startTime;

echo "Sales by Product:\n";
echo "================\n";
foreach ($salesByProduct as $data) {
    $product = $data['key'];
    $stats = $data['value'];

    echo "Product: {$product}\n";
    echo "  Total Sales: $" . number_format($stats['total']) . "\n";
    echo "  Orders: {$stats['count']}\n";
    echo "  Average: $" . number_format($stats['average'], 2) . "\n";
    echo "  Range: $" . number_format($stats['min']) . " - $" . number_format($stats['max']) . "\n";
    echo "\n";
}

echo "Execution time: " . number_format($duration, 3) . " seconds\n";

// Now analyze by region
echo "\n=== Sales by Region ===\n";

$salesByRegion = (new MapReduceBuilder())
    ->input($sales)
    ->map(function ($id, $sale) {
        yield [$sale['region'], $sale['amount']];
    })
    ->reduce(function ($region, $amounts) {
        return [
            'total' => array_sum($amounts),
            'count' => count($amounts),
            'average' => array_sum($amounts) / count($amounts),
        ];
    })
    ->concurrent(4)
    ->execute();

foreach ($salesByRegion as $data) {
    $region = $data['key'];
    $stats = $data['value'];

    echo "Region: {$region}\n";
    echo "  Total Sales: $" . number_format($stats['total']) . "\n";
    echo "  Orders: {$stats['count']}\n";
    echo "  Average: $" . number_format($stats['average'], 2) . "\n";
    echo "\n";
}