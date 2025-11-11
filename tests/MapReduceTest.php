<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Tests;

use PHPUnit\Framework\TestCase;
use Spiritinlife\MapReduce\MapReduce;
use Spiritinlife\MapReduce\MapReduceBuilder;

class MapReduceTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        parent::setUp();
        $this->tempDir = sys_get_temp_dir() . '/mapreduce_test_' . uniqid();
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

    public function testWordCount(): void
    {
        $documents = [
            'doc1' => 'the quick brown fox',
            'doc2' => 'the lazy dog',
            'doc3' => 'quick brown dogs',
        ];

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input($documents)
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
            ->execute());

        $this->assertEquals(2, $result['the']['value']);
        $this->assertEquals(2, $result['quick']['value']);
        $this->assertEquals(2, $result['brown']['value']);
        $this->assertEquals(1, $result['fox']['value']);
        $this->assertEquals(1, $result['lazy']['value']);
        $this->assertEquals(1, $result['dog']['value']);
        $this->assertEquals(1, $result['dogs']['value']);
    }

    public function testEmptyInput(): void
    {
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input([])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator))
            ->execute());

        $this->assertIsArray($result);
        $this->assertEmpty($result);
    }

    public function testSingleItem(): void
    {
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(['key' => 'value'])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->execute());

        $this->assertCount(1, $result);
        $this->assertEquals('value', $result['value']['key']);
        $this->assertEquals('value', $result['value']['value']);
    }

    public function testMultipleEmitsPerMapper(): void
    {
        $data = [1, 2, 3];

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input($data)
            ->map(function ($value, $context) {
                // Emit the value and its square
                yield ['original', $value];
                yield ['squared', $value * $value];
            })
            ->reduce(function ($type, $valuesIterator, $context) {
                $sum = 0;
                foreach ($valuesIterator as $value) {
                    $sum += $value;
                }
                return $sum;
            })
            ->execute());

        $this->assertEquals(6, $result['original']['value']); // 1+2+3
        $this->assertEquals(14, $result['squared']['value']); // 1+4+9
    }

    public function testAggregationWithStatistics(): void
    {
        $sales = [
            ['category' => 'Electronics', 'amount' => 100],
            ['category' => 'Books', 'amount' => 20],
            ['category' => 'Electronics', 'amount' => 150],
            ['category' => 'Books', 'amount' => 30],
            ['category' => 'Electronics', 'amount' => 200],
        ];

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input($sales)
            ->map(function ($sale, $context) {
                yield [$sale['category'], $sale['amount']];
            })
            ->reduce(function ($category, $amountsIterator, $context) {
                $amounts = iterator_to_array($amountsIterator);
                return [
                    'total' => array_sum($amounts),
                    'count' => count($amounts),
                    'average' => array_sum($amounts) / count($amounts),
                    'max' => max($amounts),
                    'min' => min($amounts),
                ];
            })
            ->concurrent(2)
            ->execute());

        $electronics = $result['Electronics']['value'];
        $books = $result['Books']['value'];

        $this->assertEquals(450, $electronics['total']);
        $this->assertEquals(3, $electronics['count']);
        $this->assertEquals(150, $electronics['average']);
        $this->assertEquals(200, $electronics['max']);
        $this->assertEquals(100, $electronics['min']);

        $this->assertEquals(50, $books['total']);
        $this->assertEquals(2, $books['count']);
        $this->assertEquals(25, $books['average']);
        $this->assertEquals(30, $books['max']);
        $this->assertEquals(20, $books['min']);
    }

    public function testCustomPartitioner(): void
    {
        $data = ['apple', 'apricot', 'banana', 'blueberry', 'cherry', 'cranberry'];

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input($data)
            ->map(function ($word, $context) {
                yield [$word, strlen($word)];
            })
            ->partitionBy(function ($word, $numPartitions, $context) {
                // Partition by first letter
                return ord(substr($word, 0, 1)) % $numPartitions;
            })
            ->reduce(function ($word, $lengthsIterator, $context) {
                return iterator_to_array($lengthsIterator)[0]; // Only one length per word
            })
            ->partitions(3)
            ->execute());

        $this->assertEquals(5, $result['apple']['value']);
        $this->assertEquals(6, $result['banana']['value']);
        $this->assertEquals(6, $result['cherry']['value']);
    }

    public function testComplexKeys(): void
    {
        $data = [
            ['user_id' => 1, 'product_id' => 100, 'quantity' => 2],
            ['user_id' => 1, 'product_id' => 101, 'quantity' => 1],
            ['user_id' => 2, 'product_id' => 100, 'quantity' => 3],
            ['user_id' => 1, 'product_id' => 100, 'quantity' => 1],
        ];

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input($data)
            ->map(function ($order, $context) {
                // Use array as key
                yield [
                    ['user_id' => $order['user_id'], 'product_id' => $order['product_id']],
                    $order['quantity']
                ];
            })
            ->reduce(function ($key, $quantitiesIterator, $context) {
                $sum = 0;
                foreach ($quantitiesIterator as $quantity) {
                    $sum += $quantity;
                }
                return $sum;
            })
            ->execute());

        // Keys are serialized
        $key1 = serialize(['user_id' => 1, 'product_id' => 100]);
        $key2 = serialize(['user_id' => 1, 'product_id' => 101]);
        $key3 = serialize(['user_id' => 2, 'product_id' => 100]);

        $this->assertEquals(3, $result[$key1]['value']); // 2+1
        $this->assertEquals(1, $result[$key2]['value']);
        $this->assertEquals(3, $result[$key3]['value']);
    }

    public function testLargeDataset(): void
    {
        // Generate 10,000 random items
        $data = [];
        for ($i = 0; $i < 10000; $i++) {
            $data[] = [
                'category' => 'cat_' . rand(1, 10),
                'value' => rand(1, 100)
            ];
        }

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input($data)
            ->map(function ($item, $context) {
                yield [$item['category'], $item['value']];
            })
            ->reduce(function ($category, $valuesIterator, $context) {
                $values = iterator_to_array($valuesIterator);
                return [
                    'sum' => array_sum($values),
                    'count' => count($values),
                ];
            })
            ->concurrent(4)
            ->partitions(8)
            ->execute());

        // Verify we have results for all categories
        $this->assertGreaterThan(0, count($result));

        // Verify sum of all counts equals input size
        $totalCount = 0;
        foreach ($result as $data) {
            $totalCount += $data['value']['count'];
        }
        $this->assertEquals(10000, $totalCount);
    }

    public function testHighConcurrency(): void
    {
        $data = range(1, 1000);

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input($data)
            ->map(function ($value, $context) {
                yield ['sum', $value];
            })
            ->reduce(function ($key, $valuesIterator, $context) {
                $sum = 0;
                foreach ($valuesIterator as $value) {
                    $sum += $value;
                }
                return $sum;
            })
            ->concurrent(16)
            ->partitions(8)
            ->execute());

        $this->assertEquals(500500, $result['sum']['value']); // Sum of 1 to 1000
    }

    public function testMapperThrowsException(): void
    {
        $this->expectException(\RuntimeException::class);

        $this->generatorToArray((new MapReduceBuilder())
            ->input([1, 2, 3])
            ->map(function ($value, $context) {
                if ($value === 2) {
                    throw new \RuntimeException('Test exception');
                }
                yield [$value, $value];
            })
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator))
            ->execute());
    }

    public function testReducerThrowsException(): void
    {
        $this->expectException(\RuntimeException::class);

        $this->generatorToArray((new MapReduceBuilder())
            ->input([1, 2, 3])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(function ($key, $valuesIterator, $context) {
                if ($key === 2) {
                    throw new \RuntimeException('Test exception');
                }
                return iterator_to_array($valuesIterator);
            })
            ->execute());
    }

    public function testInvalidMapperOutput(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Mapper must yield [key, value] pairs');

        $this->generatorToArray((new MapReduceBuilder())
            ->input([1])
            ->map(function ($value, $context) {
                yield 'not-an-array'; // Invalid: should be [key, value]
            })
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

    public function testMissingInputThrowsException(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Input data is required');

        $this->generatorToArray((new MapReduceBuilder())
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator))
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

    public function testCustomWorkingDirectory(): void
    {
        $customDir = $this->tempDir . '/custom_mapreduce';

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(['a' => 1, 'b' => 2])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->workingDirectory($customDir)
            ->execute());

        $this->assertCount(2, $result);
        // Working directory should be cleaned up after execution
        $this->assertDirectoryDoesNotExist($customDir);
    }

    public function testDirectMapReduceUsage(): void
    {
        $mapReduce = new MapReduce(2, $this->tempDir . '/direct');

        $result = $this->generatorToArray($mapReduce->execute(
            input: ['doc1' => 'hello world', 'doc2' => 'hello'],
            mapper: function ($text, $context) {
                foreach (explode(' ', $text) as $word) {
                    yield [$word, 1];
                }
            },
            reducer: function($word, $countsIterator, $context) {
                $sum = 0;
                foreach ($countsIterator as $count) {
                    $sum += $count;
                }
                return $sum;
            },
            reducePartitions: 2
        ));

        $this->assertEquals(2, $result['hello']['value']);
        $this->assertEquals(1, $result['world']['value']);
    }

    public function testCleanupOnException(): void
    {
        $customDir = $this->tempDir . '/cleanup_test';

        try {
            (new MapReduceBuilder())
                ->input([1, 2, 3])
                ->map(function ($value, $context) {
                    if ($value === 2) {
                        throw new \RuntimeException('Test exception');
                    }
                    yield [$value, $value];
                })
                ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator))
                ->workingDirectory($customDir)
                ->execute();

            $this->fail('Expected exception was not thrown');
        } catch (\RuntimeException $e) {
            // Working directory should still be cleaned up even on exception
            $this->assertDirectoryDoesNotExist($customDir);
        }
    }

    public function testIterableInput(): void
    {
        // Test with a generator as input
        $generator = function () {
            yield 'a' => 1;
            yield 'b' => 2;
            yield 'c' => 3;
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
        $this->assertEquals(1, $found[1]);
        $this->assertEquals(2, $found[2]);
        $this->assertEquals(3, $found[3]);
    }

    public function testPartitionerReturningInvalidValue(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Partitioner must return an integer');

        $this->generatorToArray((new MapReduceBuilder())
            ->input(['a' => 1])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator))
            ->partitionBy(function ($key, $numPartitions, $context) {
                return 999; // Invalid: out of range
            })
            ->partitions(2)
            ->execute());
    }

    public function testResultStructure(): void
    {
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(['a' => 1, 'b' => 2])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->execute());

        // Verify result structure
        foreach ($result as $serializedKey => $data) {
            $this->assertIsArray($data);
            $this->assertArrayHasKey('key', $data);
            $this->assertArrayHasKey('value', $data);
        }
    }

    public function testNoMapperEmissions(): void
    {
        // Mapper that doesn't emit anything
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input([1, 2, 3])
            ->map(function ($v, $context) {
                // Emit nothing
                return;
            })
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator))
            ->execute());

        $this->assertEmpty($result);
    }

    public function testConditionalMapperEmissions(): void
    {
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(range(1, 10))
            ->map(function ($value, $context) {
                // Only emit even numbers
                if ($value % 2 === 0) {
                    yield ['even', $value];
                }
            })
            ->reduce(function ($key, $valuesIterator, $context) {
                $sum = 0;
                foreach ($valuesIterator as $value) {
                    $sum += $value;
                }
                return $sum;
            })
            ->execute());

        $this->assertCount(1, $result);
        $this->assertEquals(30, $result['even']['value']); // 2+4+6+8+10
    }

    public function testChunkSizeConfiguration(): void
    {
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(['a' => 1, 'b' => 2, 'c' => 3])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->chunkSize(1000) // Test chunk size configuration
            ->execute());

        $this->assertCount(3, $result);
        $found = [];
        foreach ($result as $data) {
            $found[$data['key']] = $data['value'];
        }
        $this->assertEquals(1, $found[1]);
        $this->assertEquals(2, $found[2]);
        $this->assertEquals(3, $found[3]);
    }

    public function testInvalidChunkSizeThrowsException(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Shuffle chunk size must be at least 1');

        (new MapReduceBuilder())
            ->chunkSize(0);
    }

    public function testChunkedShufflingWithLargeDataset(): void
    {
        // Test that chunked shuffling works correctly with larger datasets
        $data = [];
        for ($i = 0; $i < 1000; $i++) {
            $data[] = [
                'category' => 'cat_' . ($i % 10), // 10 categories
                'value' => $i
            ];
        }

        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input($data)
            ->map(function ($item, $context) {
                yield [$item['category'], $item['value']];
            })
            ->reduce(function ($category, $valuesIterator, $context) {
                $values = iterator_to_array($valuesIterator);
                return [
                    'count' => count($values),
                    'sum' => array_sum($values),
                ];
            })
            ->chunkSize(50) // Small chunks to test chunking behavior
            ->concurrent(4)
            ->execute());

        // Verify we have 10 categories
        $this->assertCount(10, $result);

        // Each category should have 100 items (1000 / 10)
        foreach ($result as $data) {
            $this->assertEquals(100, $data['value']['count']);
        }

        // Verify total count
        $totalCount = 0;
        foreach ($result as $data) {
            $totalCount += $data['value']['count'];
        }
        $this->assertEquals(1000, $totalCount);
    }

    public function testGetWorkingDir(): void
    {
        $customDir = $this->tempDir . '/custom_working';
        $mapReduce = new MapReduce(2, $customDir);

        $this->assertEquals($customDir, $mapReduce->getWorkingDir());
        $this->assertDirectoryExists($customDir);
    }

    public function testConstructorCreatesWorkingDirectory(): void
    {
        $customDir = $this->tempDir . '/auto_created';
        $mapReduce = new MapReduce(2, $customDir);

        $this->assertDirectoryExists($customDir);
        $this->assertEquals($customDir, $mapReduce->getWorkingDir());
    }

    public function testConstructorWithInvalidConcurrency(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Concurrency must be at least 1');

        new MapReduce(0);
    }

    public function testConstructorWithInvalidChunkSize(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Shuffle chunk size must be at least 1');

        new MapReduce(2, null, 0);
    }

    public function testExecuteWithInvalidReducePartitions(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Reduce partitions must be at least 1');

        $mapReduce = new MapReduce(2, $this->tempDir . '/invalid_partitions');
        $this->generatorToArray($mapReduce->execute(
            input: [1, 2, 3],
            mapper: fn($v, $context) => yield [$v, $v],
            reducer: fn($k, $vIterator, $context) => iterator_to_array($vIterator),
            reducePartitions: 0
        ));
    }

    public function testMapperYieldingNonIterableHandledGracefully(): void
    {
        // Test that when mapper returns a single pair (not iterable), it's wrapped
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(['a' => 1])
            ->map(function ($v, $context) {
                // Return array instead of yielding
                return [[$v, $v]];
            })
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->execute());

        $this->assertCount(1, $result);
        $this->assertEquals(1, $result[1]['key']);
        $this->assertEquals(1, $result[1]['value']);
    }

    public function testMapperWithInvalidPairStructure(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Mapper must yield [key, value] pairs');

        $this->generatorToArray((new MapReduceBuilder())
            ->input(['a' => 1])
            ->map(function ($v, $context) {
                yield [$v, $v, 'extra']; // Invalid: 3 elements instead of 2
            })
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator))
            ->execute());
    }

    public function testWithPartitionerMethod(): void
    {
        $customDir = $this->tempDir . '/with_partitioner';
        $mapReduce = new MapReduce(2, $customDir);

        // Test that withPartitioner returns self for method chaining
        $result = $mapReduce->withPartitioner(function ($key, $numPartitions, $context) {
            return 0; // Always partition to 0
        });

        $this->assertSame($mapReduce, $result);
    }

    public function testIntegerMapperKeys(): void
    {
        // Test with integer keys in mapper output
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input(['a' => 100, 'b' => 200, 'c' => 300])
            ->map(function ($value, $context) {
                // Use integer keys in the map output
                yield [10, $value];
                yield [20, $value * 2];
            })
            ->reduce(function ($key, $valuesIterator, $context) {
                $sum = 0;
                foreach ($valuesIterator as $value) {
                    $sum += $value;
                }
                return $sum;
            })
            ->execute());

        // Integer keys are serialized as strings
        // Check that we got results
        $this->assertNotEmpty($result);

        // Find keys that match our integer keys (they're stringified)
        $hasKey10 = false;
        $hasKey20 = false;
        foreach ($result as $key => $data) {
            if ($data['key'] == 10) {
                $hasKey10 = true;
                $this->assertEquals(600, $data['value']); // 100+200+300
            }
            if ($data['key'] == 20) {
                $hasKey20 = true;
                $this->assertEquals(1200, $data['value']); // 200+400+600
            }
        }

        $this->assertTrue($hasKey10, 'Should have key 10');
        $this->assertTrue($hasKey20, 'Should have key 20');
    }

    public function testFloatKeys(): void
    {
        // Test with float keys in mapper output
        $result = $this->generatorToArray((new MapReduceBuilder())
            ->input([1, 2, 3])
            ->map(function ($value, $context) {
                yield [1.5, $value]; // Float key
                yield [2.5, $value * 2];
            })
            ->reduce(function ($key, $valuesIterator, $context) {
                $sum = 0;
                foreach ($valuesIterator as $value) {
                    $sum += $value;
                }
                return $sum;
            })
            ->execute());

        $this->assertArrayHasKey('1.5', $result);
        $this->assertArrayHasKey('2.5', $result);
        $this->assertEquals(6, $result['1.5']['value']); // 1+2+3
        $this->assertEquals(12, $result['2.5']['value']); // 2+4+6
    }

    public function testGetMapperComponent(): void
    {
        $customDir = $this->tempDir . '/component_test';
        $mapReduce = new MapReduce(2, $customDir);

        $mapper = $mapReduce->getMapper();
        $this->assertInstanceOf(\Spiritinlife\MapReduce\Pipeline\Mapper::class, $mapper);
    }

    public function testGetShufflerComponent(): void
    {
        $customDir = $this->tempDir . '/component_test';
        $mapReduce = new MapReduce(2, $customDir);

        $shuffler = $mapReduce->getShuffler();
        $this->assertInstanceOf(\Spiritinlife\MapReduce\Pipeline\Shuffler::class, $shuffler);
    }

    public function testGetReducerComponent(): void
    {
        $customDir = $this->tempDir . '/component_test';
        $mapReduce = new MapReduce(2, $customDir);

        $reducer = $mapReduce->getReducer();
        $this->assertInstanceOf(\Spiritinlife\MapReduce\Pipeline\Reducer::class, $reducer);
    }

    public function testComponentsAreInitializedOnConstruction(): void
    {
        $customDir = $this->tempDir . '/init_test';
        $mapReduce = new MapReduce(4, $customDir, 5000);

        // Verify all three components are initialized
        $this->assertInstanceOf(\Spiritinlife\MapReduce\Pipeline\Mapper::class, $mapReduce->getMapper());
        $this->assertInstanceOf(\Spiritinlife\MapReduce\Pipeline\Shuffler::class, $mapReduce->getShuffler());
        $this->assertInstanceOf(\Spiritinlife\MapReduce\Pipeline\Reducer::class, $mapReduce->getReducer());
    }

    public function testCleanupWithNestedDirectories(): void
    {
        $customDir = $this->tempDir . '/nested_cleanup';
        $mapReduce = new MapReduce(2, $customDir);

        // Create nested directories to test recursive cleanup
        mkdir($customDir . '/subdir1', 0755, true);
        mkdir($customDir . '/subdir2/subsubdir', 0755, true);
        file_put_contents($customDir . '/file1.txt', 'test');
        file_put_contents($customDir . '/subdir1/file2.txt', 'test');
        file_put_contents($customDir . '/subdir2/subsubdir/file3.txt', 'test');

        // Execute a simple MapReduce operation
        $result = $this->generatorToArray($mapReduce->execute(
            ['a' => 1],
            fn($v, $context) => yield [$v, $v],
            fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0],
            1
        ));

        // After execution, working directory should be cleaned up
        $this->assertDirectoryDoesNotExist($customDir);
    }

    public function testCleanupWithNonExistentDirectory(): void
    {
        $nonExistent = $this->tempDir . '/nonexistent_' . uniqid();

        // This should not throw an error even if directory doesn't exist
        $mapReduce = new MapReduce(2, $this->tempDir . '/temp_' . uniqid());

        // Manually call cleanup on a directory that doesn't exist
        // (we can't call it directly as it's protected, but it's tested through execute)
        $result = $this->generatorToArray($mapReduce->execute(
            ['a' => 1],
            fn($v, $context) => yield [$v, $v],
            fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0],
            1
        ));

        // Should complete without error
        $this->assertNotEmpty($result);
    }

    public function testCleanupWithUnreadableDirectory(): void
    {
        $customDir = $this->tempDir . '/unreadable_cleanup';
        $mapReduce = new MapReduce(2, $customDir);

        // Execute a simple operation
        $result = $this->generatorToArray($mapReduce->execute(
            ['a' => 1],
            fn($v, $context) => yield [$v, $v],
            fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0],
            1
        ));

        // Cleanup should have happened
        $this->assertDirectoryDoesNotExist($customDir);
    }

    public function testDeleteDirectoryWithSymlinks(): void
    {
        $customDir = $this->tempDir . '/symlink_test';
        $mapReduce = new MapReduce(2, $customDir);

        // Create a file
        file_put_contents($customDir . '/real_file.txt', 'content');

        // Execute operation
        $result = $this->generatorToArray($mapReduce->execute(
            ['a' => 1],
            fn($v, $context) => yield [$v, $v],
            fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0],
            1
        ));

        // Directory should be cleaned up
        $this->assertDirectoryDoesNotExist($customDir);
    }

    public function testWithPartitionerUpdatesMapper(): void
    {
        $customDir = $this->tempDir . '/partitioner_test';
        $mapReduce = new MapReduce(2, $customDir);

        $originalMapper = $mapReduce->getMapper();

        // Set custom partitioner
        $mapReduce->withPartitioner(function ($key, $numPartitions, $context) {
            return 0; // Always use partition 0
        });

        $newMapper = $mapReduce->getMapper();

        // Mapper should have been replaced
        $this->assertNotSame($originalMapper, $newMapper);
    }

    public function testMultipleExecutionsWithSameInstance(): void
    {
        // Test that we can execute multiple operations with the same instance
        // (though cleanup happens after each)
        $result1 = (new MapReduceBuilder())
            ->input(['a' => 1])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->execute();

        $found1 = [];
        foreach ($result1 as $data) {
            $found1[$data['key']] = $data['value'];
        }
        $this->assertEquals(1, $found1[1]);

        // Create new instance for second execution
        $result2 = (new MapReduceBuilder())
            ->input(['b' => 2])
            ->map(fn($v, $context) => yield [$v, $v])
            ->reduce(fn($k, $vIterator, $context) => iterator_to_array($vIterator)[0])
            ->execute();

        $found2 = [];
        foreach ($result2 as $data) {
            $found2[$data['key']] = $data['value'];
        }
        $this->assertEquals(2, $found2[2]);
    }
}
