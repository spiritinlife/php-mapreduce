<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Tests;

use PHPUnit\Framework\TestCase;
use Spiritinlife\MapReduce\MapReduceBuilder;

/**
 * Integration tests with real-world scenarios
 */
class IntegrationTest extends TestCase
{
    public function testInvertedIndexBuild(): void
    {
        $documents = [
            ['id' => 1, 'text' => 'the quick brown fox jumps over the lazy dog'],
            ['id' => 2, 'text' => 'the lazy cat sleeps on the mat'],
            ['id' => 3, 'text' => 'quick brown foxes are clever animals'],
        ];

        $invertedIndex = (new MapReduceBuilder())
            ->input($documents)
            ->map(function ($doc) {
                $words = array_unique(str_word_count(strtolower($doc['text']), 1));
                foreach ($words as $word) {
                    yield [$word, $doc['id']];
                }
            })
            ->reduce(function ($word, $docIds) {
                return [
                    'document_ids' => array_unique($docIds),
                    'document_count' => count(array_unique($docIds)),
                ];
            })
            ->concurrent(2)
            ->execute();

        // 'the' appears in documents 1 and 2 (not in document 3)
        $this->assertCount(2, $invertedIndex['the']['value']['document_ids']);
        $this->assertEquals(2, $invertedIndex['the']['value']['document_count']);
        $this->assertContains(1, $invertedIndex['the']['value']['document_ids']);
        $this->assertContains(2, $invertedIndex['the']['value']['document_ids']);

        // 'quick' appears in doc 1 and 3
        $this->assertCount(2, $invertedIndex['quick']['value']['document_ids']);
        $this->assertContains(1, $invertedIndex['quick']['value']['document_ids']);
        $this->assertContains(3, $invertedIndex['quick']['value']['document_ids']);

        // 'cat' only in doc 2
        $this->assertCount(1, $invertedIndex['cat']['value']['document_ids']);
        $this->assertContains(2, $invertedIndex['cat']['value']['document_ids']);
    }

    public function testLogAnalysis(): void
    {
        // Simulate Apache access log entries
        $logLines = [
            '192.168.1.1 - - [01/Jan/2024:10:00:00] "GET /index.html" 200 1024',
            '192.168.1.2 - - [01/Jan/2024:10:00:01] "GET /about.html" 200 2048',
            '192.168.1.1 - - [01/Jan/2024:10:00:02] "GET /contact.html" 404 512',
            '192.168.1.3 - - [01/Jan/2024:10:00:03] "POST /api/data" 200 4096',
            '192.168.1.1 - - [01/Jan/2024:10:00:04] "GET /index.html" 200 1024',
        ];

        $stats = (new MapReduceBuilder())
            ->input($logLines)
            ->map(function ($line) {
                if (preg_match('/(\S+) .*?"(\S+) (\S+).*?" (\d+) (\d+)/', $line, $m)) {
                    $ip = $m[1];
                    $method = $m[2];
                    $path = $m[3];
                    $status = (int)$m[4];
                    $bytes = (int)$m[5];

                    yield ["ip:$ip", ['requests' => 1, 'bytes' => $bytes]];
                    yield ["status:$status", ['count' => 1]];
                    yield ["path:$path", ['hits' => 1, 'bytes' => $bytes]];
                }
            })
            ->reduce(function ($key, $metrics) {
                $result = [];
                foreach ($metrics as $metric) {
                    foreach ($metric as $field => $value) {
                        $result[$field] = ($result[$field] ?? 0) + $value;
                    }
                }
                return $result;
            })
            ->concurrent(2)
            ->execute();

        // IP 192.168.1.1 made 3 requests
        $this->assertEquals(3, $stats['ip:192.168.1.1']['value']['requests']);

        // Status 200 occurred 4 times
        $this->assertEquals(4, $stats['status:200']['value']['count']);

        // /index.html was hit twice
        $this->assertEquals(2, $stats['path:/index.html']['value']['hits']);
    }

    public function testJoinOperation(): void
    {
        $users = [
            ['user_id' => 1, 'name' => 'Alice'],
            ['user_id' => 2, 'name' => 'Bob'],
        ];

        $orders = [
            ['order_id' => 101, 'user_id' => 1, 'amount' => 100],
            ['order_id' => 102, 'user_id' => 1, 'amount' => 150],
            ['order_id' => 103, 'user_id' => 2, 'amount' => 200],
        ];

        // Tag and combine both datasets
        $combined = [];
        foreach ($users as $user) {
            $combined[] = ['type' => 'user', 'data' => $user];
        }
        foreach ($orders as $order) {
            $combined[] = ['type' => 'order', 'data' => $order];
        }

        $joined = (new MapReduceBuilder())
            ->input($combined)
            ->map(function ($record) {
                if ($record['type'] === 'user') {
                    yield [$record['data']['user_id'], ['user' => $record['data']]];
                } else {
                    yield [$record['data']['user_id'], ['order' => $record['data']]];
                }
            })
            ->reduce(function ($userId, $records) {
                $user = null;
                $orders = [];

                foreach ($records as $record) {
                    if (isset($record['user'])) {
                        $user = $record['user'];
                    } else {
                        $orders[] = $record['order'];
                    }
                }

                return [
                    'user' => $user,
                    'orders' => $orders,
                    'order_count' => count($orders),
                    'total_spent' => array_sum(array_column($orders, 'amount')),
                ];
            })
            ->execute();

        // Find Alice and Bob in the results (order may vary due to partitioning)
        $alice = null;
        $bob = null;

        foreach ($joined as $data) {
            $user = $data['value'];
            if ($user['user']['name'] === 'Alice') {
                $alice = $user;
            } elseif ($user['user']['name'] === 'Bob') {
                $bob = $user;
            }
        }

        // User Alice has 2 orders totaling 250
        $this->assertNotNull($alice, 'Alice should be found in results');
        $this->assertEquals('Alice', $alice['user']['name']);
        $this->assertEquals(2, $alice['order_count']);
        $this->assertEquals(250, $alice['total_spent']);

        // User Bob has 1 order of 200
        $this->assertNotNull($bob, 'Bob should be found in results');
        $this->assertEquals('Bob', $bob['user']['name']);
        $this->assertEquals(1, $bob['order_count']);
        $this->assertEquals(200, $bob['total_spent']);
    }

    public function testGroupByWithMultipleAggregations(): void
    {
        $transactions = [
            ['date' => '2024-01-01', 'category' => 'Food', 'amount' => 50],
            ['date' => '2024-01-01', 'category' => 'Transport', 'amount' => 20],
            ['date' => '2024-01-02', 'category' => 'Food', 'amount' => 30],
            ['date' => '2024-01-02', 'category' => 'Food', 'amount' => 40],
            ['date' => '2024-01-03', 'category' => 'Transport', 'amount' => 25],
        ];

        $dailyStats = (new MapReduceBuilder())
            ->input($transactions)
            ->map(function ($transaction) {
                yield [$transaction['date'], $transaction['amount']];
            })
            ->reduce(function ($date, $amounts) {
                sort($amounts);
                return [
                    'total' => array_sum($amounts),
                    'count' => count($amounts),
                    'average' => array_sum($amounts) / count($amounts),
                    'min' => min($amounts),
                    'max' => max($amounts),
                    'median' => $amounts[intdiv(count($amounts), 2)],
                ];
            })
            ->concurrent(2)
            ->execute();

        $day1 = $dailyStats['2024-01-01']['value'];
        $this->assertEquals(70, $day1['total']);
        $this->assertEquals(2, $day1['count']);
        $this->assertEquals(35, $day1['average']);

        $day2 = $dailyStats['2024-01-02']['value'];
        $this->assertEquals(70, $day2['total']);
        $this->assertEquals(2, $day2['count']);
    }

    public function testTopNPattern(): void
    {
        // Find top 3 most frequent words
        $text = str_repeat('the ', 10) .
                str_repeat('quick ', 5) .
                str_repeat('brown ', 3) .
                str_repeat('fox ', 2) .
                str_repeat('jumps ', 1);

        $wordCounts = (new MapReduceBuilder())
            ->input(['doc' => $text])
            ->map(function ($text) {
                foreach (str_word_count(strtolower($text), 1) as $word) {
                    yield [$word, 1];
                }
            })
            ->reduce(function ($word, $counts) {
                return array_sum($counts);
            })
            ->execute();

        // Extract and sort by count
        $sorted = [];
        foreach ($wordCounts as $data) {
            $sorted[$data['key']] = $data['value'];
        }
        arsort($sorted);

        $top3 = array_slice($sorted, 0, 3, true);

        $this->assertEquals(['the' => 10, 'quick' => 5, 'brown' => 3], $top3);
    }

    public function testCooccurrenceMatrix(): void
    {
        // User interactions: each user's list of items
        $users = [
            'user1' => ['A', 'B', 'C'],
            'user2' => ['A', 'D'],
            'user3' => ['B', 'C', 'D'],
            'user4' => ['A', 'B'],
        ];

        $cooccurrences = (new MapReduceBuilder())
            ->input($users)
            ->map(function ($items) {
                // Emit all pairs of items this user interacted with
                $count = count($items);
                for ($i = 0; $i < $count; $i++) {
                    for ($j = $i + 1; $j < $count; $j++) {
                        $pair = [$items[$i], $items[$j]];
                        sort($pair);
                        yield [implode(',', $pair), 1];
                    }
                }
            })
            ->reduce(function ($pair, $counts) {
                return array_sum($counts);
            })
            ->execute();

        // A and B co-occurred in user1 and user4 = 2 times
        $this->assertEquals(2, $cooccurrences['A,B']['value']);

        // B and C co-occurred in user1 and user3 = 2 times
        $this->assertEquals(2, $cooccurrences['B,C']['value']);

        // A and C co-occurred only in user1 = 1 time
        $this->assertEquals(1, $cooccurrences['A,C']['value']);
    }

    public function testPerformanceWithLargeDataset(): void
    {
        // Generate 50,000 records
        $records = [];
        for ($i = 0; $i < 50000; $i++) {
            $records[] = [
                'category' => 'cat_' . ($i % 100),
                'value' => $i,
            ];
        }

        $startTime = microtime(true);

        $result = (new MapReduceBuilder())
            ->input($records)
            ->map(function ($record) {
                yield [$record['category'], $record['value']];
            })
            ->reduce(function ($category, $values) {
                return [
                    'count' => count($values),
                    'sum' => array_sum($values),
                ];
            })
            ->concurrent(8)
            ->partitions(16)
            ->execute();

        $duration = microtime(true) - $startTime;

        // Should complete in reasonable time (< 10 seconds)
        $this->assertLessThan(10, $duration);

        // Verify correctness
        $this->assertCount(100, $result); // 100 categories

        $totalRecords = 0;
        foreach ($result as $data) {
            $totalRecords += $data['value']['count'];
        }
        $this->assertEquals(50000, $totalRecords);
    }
}
