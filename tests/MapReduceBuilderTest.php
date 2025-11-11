<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Tests;

use PHPUnit\Framework\TestCase;
use Spiritinlife\MapReduce\MapReduceBuilder;

class MapReduceBuilderTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        parent::setUp();
        $this->tempDir = sys_get_temp_dir() . '/builder_test_' . uniqid();
        mkdir($this->tempDir, 0755, true);
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        if (is_dir($this->tempDir)) {
            $this->recursiveDelete($this->tempDir);
        }
    }

    private function recursiveDelete(string $dir): void
    {
        $files = scandir($dir);
        if ($files !== false) {
            $files = array_diff($files, ['.', '..']);
            foreach ($files as $file) {
                $path = "$dir/$file";
                is_dir($path) ? $this->recursiveDelete($path) : unlink($path);
            }
        }
        rmdir($dir);
    }

    /**
     * Convert generator results to array for easier testing
     *
     * @param \Generator $generator Generator from execute()
     * @return array<string, array{key: mixed, value: mixed}>
     */
    private function generatorToArray(\Generator $generator): array
    {
        $result = [];
        foreach ($generator as $key => $data) {
            $result[$key] = $data;
        }
        return $result;
    }

    public function testFluentInterface(): void
    {
        $builder = new MapReduceBuilder();

        // All methods should return the builder instance for chaining
        $this->assertSame($builder, $builder->input([1, 2, 3]));
        $this->assertSame($builder, $builder->map(fn($v, $context) => yield [$v, $v]));
        $this->assertSame($builder, $builder->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)));
        $this->assertSame($builder, $builder->concurrent(4));
        $this->assertSame($builder, $builder->partitions(8));
        $this->assertSame($builder, $builder->chunkSize(1000));
        $this->assertSame($builder, $builder->workingDirectory($this->tempDir));
        $this->assertSame($builder, $builder->partitionBy(fn($k, $n, $context) => 0));
    }

    public function testInputConfiguration(): void
    {
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(['a' => 1, 'b' => 2])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->execute());

        $this->assertCount(2, $result);
        // Find values in results
        $found = [];
        foreach ($result as $data) {
            $found[$data['key']] = $data['value'];
        }
        $this->assertEquals(1, $found[1]);
        $this->assertEquals(2, $found[2]);
    }

    public function testConcurrentConfiguration(): void
    {
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(range(1, 10))
            ->map(fn($v, $context) => yield ['sum', $v])
            ->reduce(function($_k, $vIterator, $context) {
                $sum = 0;
                foreach ($vIterator as $value) {
                    $sum += $value;
                }
                return $sum;
            })
            ->concurrent(2) // 2 workers
            ->execute());

        $this->assertEquals(55, $result['sum']['value']); // Sum of 1..10
    }

    public function testPartitionsConfiguration(): void
    {
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(['a' => 1, 'b' => 2, 'c' => 3])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->partitions(4) // 4 reduce partitions
            ->execute());

        $this->assertCount(3, $result);
    }

    public function testChunkSizeConfiguration(): void
    {
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(range(1, 100))
            ->map(fn($v, $context) => yield ['total', $v])
            ->reduce(function($_k, $vIterator, $context) {
                $sum = 0;
                foreach ($vIterator as $value) {
                    $sum += $value;
                }
                return $sum;
            })
            ->chunkSize(10) // Small chunks
            ->execute());

        $this->assertEquals(5050, $result['total']['value']); // Sum of 1..100
    }

    public function testWorkingDirectoryConfiguration(): void
    {
        $customDir = $this->tempDir . '/custom_work';

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(['x' => 1])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->workingDirectory($customDir)
            ->execute());

        $this->assertEquals(1, $result[1]['key']);
        $this->assertEquals(1, $result[1]['value']);
        // Directory should be cleaned up after execution
        $this->assertDirectoryDoesNotExist($customDir);
    }

    public function testPartitionByConfiguration(): void
    {
        // Custom partitioner that groups by first letter
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(['apple', 'apricot', 'banana', 'blueberry'])
            ->map(fn($word, $context) => yield [$word, strlen($word)])
            ->partitionBy(function ($word, $numPartitions, $context) {
                return ord(substr($word, 0, 1)) % $numPartitions;
            })
            ->reduce(fn($word, $lengthsIterator, $context) => iterator_to_array($lengthsIterator)[0])
            ->partitions(3)
            ->execute());

        $this->assertEquals(5, $result['apple']['value']);
        $this->assertEquals(6, $result['banana']['value']);
    }

    public function testMissingInputThrowsException(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Input data is required');

        $this->generatorToArray((new MapReduceBuilder())
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator))
            ->execute());
    }

    public function testMissingMapperThrowsException(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Mapper function is required');

        $this->generatorToArray((new MapReduceBuilder())
            ->input([1, 2, 3])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator))
            ->execute());
    }

    public function testMissingReducerThrowsException(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Reducer function is required');

        $this->generatorToArray((new MapReduceBuilder())
            ->input([1, 2, 3])
            ->map(fn($v, $context) => yield [$v, $v])
            ->execute());
    }

    public function testInvalidConcurrencyThrowsException(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Concurrency must be at least 1');

        (new MapReduceBuilder())
            ->concurrent(0);
    }

    public function testInvalidPartitionsThrowsException(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Partitions must be at least 1');

        (new MapReduceBuilder())
            ->partitions(0);
    }

    public function testInvalidChunkSizeThrowsException(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Shuffle chunk size must be at least 1');

        (new MapReduceBuilder())
            ->chunkSize(0);
    }

    public function testCompleteWorkflow(): void
    {
        // Test a complete workflow with all configuration options
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input([
                'doc1' => 'the quick brown fox',
                'doc2' => 'the lazy dog',
                'doc3' => 'quick brown dogs',
            ])
            ->map(function ($text, $context) {
                $words = str_word_count(strtolower($text), 1);
                foreach ($words as $word) {
                    yield [$word, 1];
                }
            })
            ->reduce(function ($word, $countsIterator, $context) {
                $sum = 0;
                foreach ($countsIterator as $count) {
                    $sum += $count;
                }
                return $sum;
            })
            ->concurrent(2)
            ->partitions(4)
            ->chunkSize(100)
            ->workingDirectory($this->tempDir . '/complete')
            ->execute());

        // Verify word counts
        $this->assertEquals(2, $result['the']['value']);
        $this->assertEquals(2, $result['quick']['value']);
        $this->assertEquals(2, $result['brown']['value']);
        $this->assertEquals(1, $result['fox']['value']);
        $this->assertEquals(1, $result['lazy']['value']);
        $this->assertEquals(1, $result['dog']['value']);
        $this->assertEquals(1, $result['dogs']['value']);
    }

    public function testDefaultConfiguration(): void
    {
        // Test with minimal configuration (using defaults)
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(['a' => 1, 'b' => 2])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->execute());

        $this->assertCount(2, $result);
    }

    public function testIterableInput(): void
    {
        // Test with generator input
        $generator = function () {
            yield 'x' => 10;
            yield 'y' => 20;
            yield 'z' => 30;
        };

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input($generator())
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->execute());

        $this->assertCount(3, $result);
        $found = [];
        foreach ($result as $data) {
            $found[$data['key']] = $data['value'];
        }
        $this->assertEquals(10, $found[10]);
        $this->assertEquals(20, $found[20]);
        $this->assertEquals(30, $found[30]);
    }

    public function testEmptyInput(): void
    {
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input([])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator))
            ->execute());

        $this->assertEmpty($result);
    }

    public function testMethodChainingOrder(): void
    {
        // Test that methods can be called in any order
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->concurrent(2)
            ->partitions(4)
            ->input(['a' => 1])
            ->chunkSize(100)
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->map(fn($v, $context) => yield [$v, $v])
            ->execute());

        $this->assertEquals(1, $result[1]['key']);
        $this->assertEquals(1, $result[1]['value']);
    }

    public function testContextFluentInterface(): void
    {
        $builder = new MapReduceBuilder();
        $context = ['threshold' => 5];

        // context() should return the builder instance for chaining
        $this->assertSame($builder, $builder->context($context));
    }

    public function testContextInMapper(): void
    {
        $context = ['threshold' => 5];

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(range(1, 10))
            ->context($context)
            ->map(function ($value, $ctx) {
                // Only emit values above threshold
                if ($value > $ctx['threshold']) {
                    yield ['high', $value];
                }
            })
            ->reduce(function($_k, $vIterator) {
                $sum = 0;
                foreach ($vIterator as $value) {
                    $sum += $value;
                }
                return $sum;
            })
            ->execute());

        // Should only include 6, 7, 8, 9, 10 (values > 5)
        $this->assertEquals(40, $result['high']['value']); // 6+7+8+9+10 = 40
    }

    public function testContextInReducer(): void
    {
        $context = ['multiplier' => 2];

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input([1, 2, 3, 4, 5])
            ->context($context)
            ->map(fn($v) => yield ['sum', $v])
            ->reduce(function ($key, $valuesIterator, $ctx) {
                // Use context in reducer
                $sum = 0;
                foreach ($valuesIterator as $value) {
                    $sum += $value;
                }
                return $sum * $ctx['multiplier'];
            })
            ->execute());

        // (1+2+3+4+5) * 2 = 30
        $this->assertEquals(30, $result['sum']['value']);
    }

    public function testContextInBothMapperAndReducer(): void
    {
        $context = [
            'threshold' => 3,
            'multiplier' => 10
        ];

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(range(1, 5))
            ->context($context)
            ->map(function ($value, $ctx) {
                if ($value > $ctx['threshold']) {
                    yield ['high', $value];
                } else {
                    yield ['low', $value];
                }
            })
            ->reduce(function ($key, $valuesIterator, $ctx) {
                $sum = 0;
                foreach ($valuesIterator as $value) {
                    $sum += $value;
                }
                return $sum * $ctx['multiplier'];
            })
            ->execute());

        // High: (4+5) * 10 = 90
        // Low: (1+2+3) * 10 = 60
        $this->assertEquals(90, $result['high']['value']);
        $this->assertEquals(60, $result['low']['value']);
    }

    public function testContextWithComplexDataStructures(): void
    {
        $context = [
            'lookup' => ['a' => 1, 'b' => 2, 'c' => 3],
            'config' => ['enabled' => true, 'factor' => 5]
        ];

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(['a', 'b', 'c'])
            ->context($context)
            ->map(function ($letter, $ctx) {
                if ($ctx['config']['enabled']) {
                    yield [$letter, $ctx['lookup'][$letter]];
                }
            })
            ->reduce(function ($_key, $valuesIterator, $ctx) {
                $values = iterator_to_array($valuesIterator);
                return $values[0] * $ctx['config']['factor'];
            })
            ->execute());

        // a: 1 * 5 = 5
        // b: 2 * 5 = 10
        // c: 3 * 5 = 15
        $this->assertEquals(5, $result['a']['value']);
        $this->assertEquals(10, $result['b']['value']);
        $this->assertEquals(15, $result['c']['value']);
    }

    public function testContextWithLookupTables(): void
    {
        // Simulate the LLR use case with event counts and total users
        $eventCounts = ['follow' => ['seller1' => 10, 'seller2' => 5]];
        $totalBuyers = 100;

        $context = [
            'eventCounts' => $eventCounts,
            'totalBuyers' => $totalBuyers,
            'threshold' => 8
        ];

        $cooccurrences = [
            ['primary' => 'seller1', 'secondary' => 'seller2', 'count' => 3],
            ['primary' => 'seller1', 'secondary' => 'seller3', 'count' => 7],
        ];

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input($cooccurrences)
            ->context($context)
            ->map(function ($cooc, $_ctx) {
                $key = "{$cooc['primary']}_{$cooc['secondary']}";
                yield [$key, $cooc['count']];
            })
            ->reduce(function ($key, $countsIterator, $ctx) {
                $totalCount = 0;
                foreach ($countsIterator as $count) {
                    $totalCount += $count;
                }

                // Only return if above threshold
                if ($totalCount >= $ctx['threshold']) {
                    return [
                        'pair' => $key,
                        'count' => $totalCount,
                        'percentage' => ($totalCount / $ctx['totalBuyers']) * 100
                    ];
                }

                return null;
            })
            ->execute());

        // Both counts are below threshold (3 < 8, 7 < 8)
        // When reducer returns null, it still emits a record with value=null
        $this->assertCount(2, $result);
        $this->assertNull($result['seller1_seller2']['value']);
        $this->assertNull($result['seller1_seller3']['value']);
    }

    public function testContextIsNullByDefault(): void
    {
        // Test that context works when not set (should be null)
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input([1, 2, 3])
            ->map(fn($v, $context) => yield ['sum', $v])
            ->reduce(function($_k, $vIterator, $context) {
                $sum = 0;
                foreach ($vIterator as $value) {
                    $sum += $value;
                }
                return $sum;
            })
            ->execute());

        $this->assertEquals(6, $result['sum']['value']);
    }

    public function testContextWithParallelProcessing(): void
    {
        // Test that context works correctly with multiple parallel workers
        $context = ['multiplier' => 3];

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(range(1, 100))
            ->context($context)
            ->map(function ($value, $_ctx) {
                yield ['group' . ($value % 5), $value];
            })
            ->reduce(function ($_key, $valuesIterator, $ctx) {
                $sum = 0;
                foreach ($valuesIterator as $value) {
                    $sum += $value;
                }
                return $sum * $ctx['multiplier'];
            })
            ->concurrent(4) // Multiple workers
            ->partitions(8)
            ->execute());

        // Verify all groups are present and multiplied correctly
        $this->assertCount(5, $result); // groups 0-4

        // Manually calculate expected sum for group0: 5+10+15+...+100 = 1050
        // Multiplied by 3 = 3150
        $group0Sum = 0;
        for ($i = 5; $i <= 100; $i += 5) {
            $group0Sum += $i;
        }
        $this->assertEquals($group0Sum * 3, $result['group0']['value']);
    }

    public function testContextSerializationOfObjects(): void
    {
        // Test that context can contain objects (as long as they're serializable)
        $contextObject = new \stdClass();
        $contextObject->threshold = 5;
        $contextObject->label = 'test';

        $context = ['config' => $contextObject];

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(range(1, 10))
            ->context($context)
            ->map(function ($value, $ctx) {
                if ($value > $ctx['config']->threshold) {
                    yield [$ctx['config']->label, $value];
                }
            })
            ->reduce(function($_k, $vIterator, $context) {
                $sum = 0;
                foreach ($vIterator as $value) {
                    $sum += $value;
                }
                return $sum;
            })
            ->execute());

        // Should sum values > 5: 6+7+8+9+10 = 40
        $this->assertEquals(40, $result['test']['value']);
    }
}
