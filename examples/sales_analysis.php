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
    ->map(function ($sale, $context) {
        yield [$sale['product'], $sale['amount']];
    })
    ->reduce(function ($product, $amountsIterator, $context) {
        // Use accumulator pattern to calculate statistics
        $total = 0;
        $count = 0;
        $min = PHP_INT_MAX;
        $max = PHP_INT_MIN;

        foreach ($amountsIterator as $amount) {
            $total += $amount;
            $count++;
            $min = min($min, $amount);
            $max = max($max, $amount);
        }

        return [
            'total' => $total,
            'count' => $count,
            'average' => $count > 0 ? $total / $count : 0,
            'min' => $min,
            'max' => $max,
        ];
    })
    ->concurrent(4)
    ->execute();

$duration = microtime(true) - $startTime;

echo "First stage execution time: " . number_format($duration, 3) . " seconds\n";

// Stage 2: Use results from first MapReduce as input to second MapReduce
// Feed the generator directly into the second MapReduce
echo "\n=== Stage 2: Categorize Products by Performance Tier ===\n";

$startTime2 = microtime(true);

// Second MapReduce: Categorize by performance tier
// Takes the generator from first stage directly as input (PROBLEMATIC!)
$tierAnalysis = (new MapReduceBuilder())
    ->input($salesByProduct)
    ->map(function ($data, $context) {
        // Input is the result from first MapReduce: ['key' => product, 'value' => stats]
        $product = $data['key'];
        $stats = $data['value'];
        $total = $stats['total'];

        // Categorize based on total sales
        if ($total >= 400) {
            $tier = 'High Performer';
        } elseif ($total >= 200) {
            $tier = 'Medium Performer';
        } else {
            $tier = 'Low Performer';
        }

        yield [$tier, [
            'product' => $product,
            'total_sales' => $total,
            'order_count' => $stats['count'],
        ]];
    })
    ->reduce(function ($tier, $productsIterator, $context) {
        // Collect all products in this tier
        $products = [];
        $tierTotalSales = 0;
        $tierTotalOrders = 0;

        foreach ($productsIterator as $product) {
            $products[] = $product;
            $tierTotalSales += $product['total_sales'];
            $tierTotalOrders += $product['order_count'];
        }

        return [
            'products' => $products,
            'product_count' => count($products),
            'tier_total_sales' => $tierTotalSales,
            'tier_total_orders' => $tierTotalOrders,
            'tier_average_per_product' => count($products) > 0 ? $tierTotalSales / count($products) : 0,
        ];
    })
    ->concurrent(2)
    ->execute();

$duration2 = microtime(true) - $startTime2;

// Define tier display order
$tierOrder = ['High Performer', 'Medium Performer', 'Low Performer'];
$tierResults = [];

// Collect results by tier
foreach ($tierAnalysis as $data) {
    $tierResults[$data['key']] = $data['value'];
}

echo "Performance Tiers:\n";
echo "==================\n";

// Display in order
foreach ($tierOrder as $tier) {
    if (!isset($tierResults[$tier])) {
        continue;
    }

    $stats = $tierResults[$tier];
    echo "\n{$tier}:\n";
    echo "  Number of products: {$stats['product_count']}\n";
    echo "  Tier total sales: $" . number_format($stats['tier_total_sales']) . "\n";
    echo "  Tier total orders: {$stats['tier_total_orders']}\n";
    echo "  Average per product: $" . number_format($stats['tier_average_per_product'], 2) . "\n";
    echo "  Products:\n";
    foreach ($stats['products'] as $product) {
        echo "    - {$product['product']}: $" . number_format($product['total_sales']) . " ({$product['order_count']} orders)\n";
    }
}

echo "\nSecond stage execution time: " . number_format($duration2, 3) . " seconds\n";
echo "Total pipeline execution time: " . number_format($duration + $duration2, 3) . " seconds\n";