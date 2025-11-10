<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Tests;

use PHPUnit\Framework\TestCase;
use Spiritinlife\MapReduce\Pipeline\Reducer;

class ReducerTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        parent::setUp();
        $this->tempDir = sys_get_temp_dir() . '/reducer_test_' . uniqid();
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
     * @param \Generator $generator Generator from reduce()
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

    /**
     * @param array<string, array<int, mixed>> $groups
     */
    private function createShuffledFile(string $filename, array $groups): void
    {
        $handle = fopen($filename, 'w');
        if ($handle !== false) {
            foreach ($groups as $key => $values) {
                fwrite($handle, serialize([
                    'key' => $key,
                    'values' => $values
                ]) . "\n");
            }
            fclose($handle);
        }
    }

    public function testBasicReduce(): void
    {
        $reducer = new Reducer();

        // Create shuffled files
        $file1 = "{$this->tempDir}/shuffled_0.tmp";
        $file2 = "{$this->tempDir}/shuffled_1.tmp";

        $this->createShuffledFile($file1, [
            'apple' => [1, 2, 3],
            'banana' => [4, 5],
        ]);

        $this->createShuffledFile($file2, [
            'cherry' => [6],
        ]);

        $results = $this->generatorToArray($reducer->reduce(
            [$file1, $file2],
            fn($key, $values) => array_sum($values)
        ));

        $this->assertEquals(6, $results['apple']['value']); // 1+2+3
        $this->assertEquals(9, $results['banana']['value']); // 4+5
        $this->assertEquals(6, $results['cherry']['value']); // 6
    }

    public function testEmptyInput(): void
    {
        $reducer = new Reducer();

        $results = $this->generatorToArray($reducer->reduce(
            [],
            fn($key, $values) => array_sum($values)
        ));

        $this->assertEmpty($results);
    }

    public function testSinglePartition(): void
    {
        $reducer = new Reducer();

        $file = "{$this->tempDir}/shuffled_0.tmp";
        $this->createShuffledFile($file, [
            'word1' => [1, 1, 1],
            'word2' => [2, 2],
        ]);

        $results = $this->generatorToArray($reducer->reduce(
            [$file],
            fn($key, $values) => array_sum($values)
        ));

        $this->assertEquals(3, $results['word1']['value']);
        $this->assertEquals(4, $results['word2']['value']);
    }

    public function testNonExistentFile(): void
    {
        $reducer = new Reducer();

        $file1 = "{$this->tempDir}/shuffled_0.tmp";
        $file2 = "{$this->tempDir}/nonexistent.tmp"; // Doesn't exist

        $this->createShuffledFile($file1, [
            'key' => [1, 2, 3]
        ]);

        $results = $this->generatorToArray($reducer->reduce(
            [$file1, $file2],
            fn($key, $values) => array_sum($values)
        ));

        // Should handle non-existent file gracefully
        $this->assertEquals(6, $results['key']['value']);
    }

    public function testComplexReducer(): void
    {
        $reducer = new Reducer();

        $file = "{$this->tempDir}/shuffled_0.tmp";
        $this->createShuffledFile($file, [
            'category1' => [100, 150, 200],
            'category2' => [50, 75],
        ]);

        $results = $this->generatorToArray($reducer->reduce(
            [$file],
            function ($key, $values) {
                return [
                    'sum' => array_sum($values),
                    'count' => count($values),
                    'avg' => array_sum($values) / count($values),
                    'max' => max($values),
                    'min' => min($values),
                ];
            }
        ));

        $cat1 = $results['category1']['value'];
        $this->assertEquals(450, $cat1['sum']);
        $this->assertEquals(3, $cat1['count']);
        $this->assertEquals(150, $cat1['avg']);
        $this->assertEquals(200, $cat1['max']);
        $this->assertEquals(100, $cat1['min']);

        $cat2 = $results['category2']['value'];
        $this->assertEquals(125, $cat2['sum']);
        $this->assertEquals(2, $cat2['count']);
    }

    public function testIntegerKeys(): void
    {
        $reducer = new Reducer();

        $file = "{$this->tempDir}/shuffled_0.tmp";
        
        $this->createShuffledFile($file, [
            '10' => [1, 2, 3],
            '20' => [4, 5],
        ]);

        $results = $this->generatorToArray($reducer->reduce(
            [$file],
            fn($key, $values) => array_sum($values)
        ));

        // Check results by the original key
        $found10 = false;
        $found20 = false;
        foreach ($results as $data) {
            if ($data['key'] == 10) {
                $found10 = true;
                $this->assertEquals(6, $data['value']);
            }
            if ($data['key'] == 20) {
                $found20 = true;
                $this->assertEquals(9, $data['value']);
            }
        }

        $this->assertTrue($found10, 'Should have key 10');
        $this->assertTrue($found20, 'Should have key 20');
    }

    public function testFloatKeys(): void
    {
        $reducer = new Reducer();

        $file = "{$this->tempDir}/shuffled_0.tmp";

        // Manually write file with float keys
        $handle = fopen($file, 'w');
        if ($handle !== false) {
            fwrite($handle, serialize([
                'key' => 1.5,
                'values' => [10, 20]
            ]) . "\n");
            fwrite($handle, serialize([
                'key' => 2.5,
                'values' => [30]
            ]) . "\n");
            fclose($handle);
        }

        $results = $this->generatorToArray($reducer->reduce(
            [$file],
            fn($key, $values) => array_sum($values)
        ));

        // Float keys are converted to strings in serialization
        $found15 = false;
        $found25 = false;
        foreach ($results as $serializedKey => $data) {
            $key = $data['key'];
            // Convert to float for comparison
            $keyFloat = is_numeric($key) ? (float)$key : null;

            if ($keyFloat !== null && abs($keyFloat - 1.5) < 0.001) {
                $found15 = true;
                $this->assertEquals(30, $data['value']);
            }
            if ($keyFloat !== null && abs($keyFloat - 2.5) < 0.001) {
                $found25 = true;
                $this->assertEquals(30, $data['value']);
            }
        }

        $this->assertTrue($found15, 'Should have key 1.5');
        $this->assertTrue($found25, 'Should have key 2.5');
    }

    public function testComplexKeys(): void
    {
        $reducer = new Reducer();

        $file = "{$this->tempDir}/shuffled_0.tmp";
        $complexKey1 = ['user' => 1, 'product' => 100];
        $complexKey2 = ['user' => 2, 'product' => 101];

        // Manually write file since PHP arrays can't have array keys
        $handle = fopen($file, 'w');
        if ($handle !== false) {
            fwrite($handle, serialize([
                'key' => $complexKey1,
                'values' => [2, 3]
            ]) . "\n");
            fwrite($handle, serialize([
                'key' => $complexKey2,
                'values' => [1]
            ]) . "\n");
            fclose($handle);
        }

        $results = $this->generatorToArray($reducer->reduce(
            [$file],
            fn($key, $values) => array_sum($values)
        ));

        // Find results by comparing keys
        $found1 = false;
        $found2 = false;
        foreach ($results as $data) {
            if ($data['key'] == $complexKey1) {
                $found1 = true;
                $this->assertEquals(5, $data['value']);
            }
            if ($data['key'] == $complexKey2) {
                $found2 = true;
                $this->assertEquals(1, $data['value']);
            }
        }

        $this->assertTrue($found1, 'Should have complex key 1');
        $this->assertTrue($found2, 'Should have complex key 2');
    }

    public function testReducerThrowsException(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Test exception');

        $reducer = new Reducer();

        $file = "{$this->tempDir}/shuffled_0.tmp";
        $this->createShuffledFile($file, [
            'key1' => [1, 2],
            'key2' => [3, 4],
        ]);

        $this->generatorToArray($reducer->reduce(
            [$file],
            function ($key, $values) {
                if ($key === 'key2') {
                    throw new \RuntimeException('Test exception');
                }
                return array_sum($values);
            }
        ));
    }

    public function testMultiplePartitions(): void
    {
        $reducer = new Reducer();

        // Create multiple partition files
        $file1 = "{$this->tempDir}/shuffled_0.tmp";
        $file2 = "{$this->tempDir}/shuffled_1.tmp";
        $file3 = "{$this->tempDir}/shuffled_2.tmp";

        $this->createShuffledFile($file1, [
            'key_a' => [1, 2],
        ]);

        $this->createShuffledFile($file2, [
            'key_b' => [3, 4],
            'key_c' => [5],
        ]);

        $this->createShuffledFile($file3, [
            'key_d' => [6, 7, 8],
        ]);

        $results = $this->generatorToArray($reducer->reduce(
            [$file1, $file2, $file3],
            fn($key, $values) => array_sum($values)
        ));

        $this->assertCount(4, $results);
        $this->assertEquals(3, $results['key_a']['value']);
        $this->assertEquals(7, $results['key_b']['value']);
        $this->assertEquals(5, $results['key_c']['value']);
        $this->assertEquals(21, $results['key_d']['value']);
    }

    public function testConcurrentProcessing(): void
    {
        $reducer = new Reducer();

        // Create many partition files
        $files = [];
        for ($i = 0; $i < 10; $i++) {
            $file = "{$this->tempDir}/shuffled_{$i}.tmp";
            $files[] = $file;
            $this->createShuffledFile($file, [
                "key_{$i}" => range(1, 10)
            ]);
        }

        $results = $this->generatorToArray($reducer->reduce(
            $files,
            fn($key, $values) => array_sum($values)
        ));

        // Should have 10 keys, each with sum of 1..10 = 55
        $this->assertCount(10, $results);
        foreach ($results as $data) {
            $this->assertEquals(55, $data['value']);
        }
    }

    public function testResultStructure(): void
    {
        $reducer = new Reducer();

        $file = "{$this->tempDir}/shuffled_0.tmp";
        $this->createShuffledFile($file, [
            'test_key' => [1, 2, 3]
        ]);

        $results = $this->generatorToArray($reducer->reduce(
            [$file],
            fn($key, $values) => array_sum($values)
        ));

        // Verify result structure
        $this->assertArrayHasKey('test_key', $results);
        $this->assertIsArray($results['test_key']);
        $this->assertArrayHasKey('key', $results['test_key']);
        $this->assertArrayHasKey('value', $results['test_key']);
        $this->assertEquals('test_key', $results['test_key']['key']);
        $this->assertEquals(6, $results['test_key']['value']);
    }

    public function testBooleanKeys(): void
    {
        $reducer = new Reducer();

        $file = "{$this->tempDir}/shuffled_0.tmp";

        // Manually write file with boolean keys
        $handle = fopen($file, 'w');
        if ($handle !== false) {
            fwrite($handle, serialize([
                'key' => true,
                'values' => [1, 2, 3]
            ]) . "\n");
            fwrite($handle, serialize([
                'key' => false,
                'values' => [4, 5]
            ]) . "\n");
            fclose($handle);
        }

        $results = $this->generatorToArray($reducer->reduce(
            [$file],
            fn($key, $values) => array_sum($values)
        ));

        // Boolean keys get serialized as strings "1" and ""
        $foundTrue = false;
        $foundFalse = false;
        foreach ($results as $data) {
            if ($data['key'] === true || $data['key'] == '1') {
                $foundTrue = true;
                $this->assertEquals(6, $data['value']);
            }
            if ($data['key'] === false || $data['key'] === '' || $data['key'] == '0') {
                $foundFalse = true;
                $this->assertEquals(9, $data['value']);
            }
        }

        $this->assertTrue($foundTrue || $foundFalse, 'Should handle boolean keys');
    }

    public function testStringKeysSerialization(): void
    {
        $reducer = new Reducer();

        $file = "{$this->tempDir}/shuffled_0.tmp";
        $this->createShuffledFile($file, [
            'alpha' => [10],
            'beta' => [20],
            'gamma' => [30],
        ]);

        $results = $this->generatorToArray($reducer->reduce(
            [$file],
            fn($key, $values) => array_sum($values)
        ));

        // String keys should be preserved
        $this->assertArrayHasKey('alpha', $results);
        $this->assertArrayHasKey('beta', $results);
        $this->assertArrayHasKey('gamma', $results);
        $this->assertEquals(10, $results['alpha']['value']);
        $this->assertEquals(20, $results['beta']['value']);
        $this->assertEquals(30, $results['gamma']['value']);
    }

    public function testEmptyShuffledFile(): void
    {
        $reducer = new Reducer();

        $file = "{$this->tempDir}/empty_shuffled.tmp";
        touch($file); // Create empty file

        $results = $this->generatorToArray($reducer->reduce(
            [$file],
            fn($key, $values) => array_sum($values)
        ));

        $this->assertEmpty($results);
    }
}
