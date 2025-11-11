<?php

/**
 * Example: Using Context to Pass Data to Mapper and Reducer Functions
 *
 * This example demonstrates how to use the context() method to pass
 * shared data to mapper and reducer functions executing in parallel processes.
 *
 * Before the context feature, you might try:
 *   ->reduce(function($key, $values) use ($sharedData) { ... })
 *
 * But this doesn't work in parallel processes! Instead, use:
 *   ->context($sharedData)
 *   ->reduce(function($key, $values, $context) { ... })
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Spiritinlife\MapReduce\MapReduceBuilder;

// Example 1: Simple threshold filtering with context
echo "Example 1: Threshold filtering\n";
echo "================================\n\n";

$numbers = range(1, 100);
$threshold = 50;  // This needs to be passed to the mapper

$results = (new MapReduceBuilder())
    ->input($numbers)
    ->context(['threshold' => $threshold])  // Pass threshold via context
    ->map(function ($value, $context) {
        // Access threshold from context
        if ($value > $context['threshold']) {
            yield ['high', $value];
        } else {
            yield ['low', $value];
        }
    })
    ->reduce(function ($key, $values, $context) {
        // Context is also available in reducer
        return [
            'category' => $key,
            'count' => count($values),
            'sum' => array_sum($values),
            'threshold_used' => $context['threshold']
        ];
    })
    ->concurrent(2)
    ->partitions(2)
    ->execute();

foreach ($results as $result) {
    print_r($result);
}

echo "\n\n";

// Example 2: Using lookup tables (like your LLR example)
echo "Example 2: Lookup tables (similar to LLR)\n";
echo "==========================================\n\n";

// Simulated data: user purchases
$purchases = [
    ['user' => 'alice', 'product' => 'A'],
    ['user' => 'alice', 'product' => 'B'],
    ['user' => 'bob', 'product' => 'A'],
    ['user' => 'bob', 'product' => 'C'],
    ['user' => 'charlie', 'product' => 'B'],
];

// Phase 1: Calculate product counts (simulated)
$productCounts = ['A' => 2, 'B' => 2, 'C' => 1];
$totalUsers = 3;

// Phase 2: Use counts in recommendation calculation
$recommendations = (new MapReduceBuilder())
    ->input($purchases)
    ->context([
        'productCounts' => $productCounts,
        'totalUsers' => $totalUsers,
        'minCount' => 2  // Configuration parameter
    ])
    ->map(function ($purchase, $context) {
        $product = $purchase['product'];

        // Only emit if product meets minimum count
        if ($context['productCounts'][$product] >= $context['minCount']) {
            yield [$product, $purchase['user']];
        }
    })
    ->reduce(function ($product, $users, $context) {
        $uniqueUsers = array_unique($users);
        $popularity = count($uniqueUsers) / $context['totalUsers'];

        return [
            'product' => $product,
            'users' => $uniqueUsers,
            'popularity' => $popularity,
            'total_count' => $context['productCounts'][$product]
        ];
    })
    ->concurrent(2)
    ->partitions(2)
    ->execute();

foreach ($recommendations as $rec) {
    print_r($rec);
}

echo "\n\n";

// Example 3: Complex context with nested data structures
echo "Example 3: Complex context structures\n";
echo "======================================\n\n";

$events = [
    ['type' => 'login', 'user' => 'alice', 'timestamp' => 1000],
    ['type' => 'purchase', 'user' => 'alice', 'timestamp' => 1100],
    ['type' => 'login', 'user' => 'bob', 'timestamp' => 1200],
    ['type' => 'logout', 'user' => 'alice', 'timestamp' => 1300],
];

$complexContext = [
    'config' => [
        'timeWindow' => 500,
        'eventWeights' => [
            'login' => 1,
            'purchase' => 10,
            'logout' => 1
        ]
    ],
    'userProfiles' => [
        'alice' => ['premium' => true],
        'bob' => ['premium' => false]
    ]
];

$analysis = (new MapReduceBuilder())
    ->input($events)
    ->context($complexContext)
    ->map(function ($event, $context) {
        $weight = $context['config']['eventWeights'][$event['type']] ?? 0;
        $isPremium = $context['userProfiles'][$event['user']]['premium'] ?? false;

        // Premium users get double weight
        if ($isPremium) {
            $weight *= 2;
        }

        yield [$event['user'], [
            'type' => $event['type'],
            'weight' => $weight,
            'timestamp' => $event['timestamp']
        ]];
    })
    ->reduce(function ($user, $events, $context) {
        $totalWeight = array_sum(array_column($events, 'weight'));
        $eventCount = count($events);

        return [
            'user' => $user,
            'total_weight' => $totalWeight,
            'event_count' => $eventCount,
            'is_premium' => $context['userProfiles'][$user]['premium'] ?? false
        ];
    })
    ->concurrent(2)
    ->partitions(2)
    ->execute();

foreach ($analysis as $result) {
    print_r($result);
}

echo "\nDone!\n";
