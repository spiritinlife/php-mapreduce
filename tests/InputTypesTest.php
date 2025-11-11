<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Tests;

use PHPUnit\Framework\TestCase;
use Spiritinlife\MapReduce\MapReduceBuilder;

/**
 * Tests for various input types accepted by MapReduce
 */
class InputTypesTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        parent::setUp();
        $this->tempDir = sys_get_temp_dir() . '/input_types_test_' . uniqid();
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

    public function testArrayInput(): void
    {
        $result = $this->generatorToArray(
            (new MapReduceBuilder())
                ->input(['a', 'b', 'c'])
                ->map(fn($v) => yield [$v, 1])
                ->reduce(function($k, $vIterator) {
                    $sum = 0;
                    foreach ($vIterator as $value) {
                        $sum += $value;
                    }
                    return $sum;
                })
                ->execute()
        );

        $this->assertCount(3, $result);
    }

    public function testArrayIteratorInput(): void
    {
        $data = ['x' => 10, 'y' => 20];

        $result = $this->generatorToArray(
            (new MapReduceBuilder())
                ->input(new \ArrayIterator($data))
                ->map(fn($v) => yield [$v, $v])
                ->reduce(fn($k, $vIterator) => iterator_to_array($vIterator)[0])
                ->execute()
        );

        $found = [];
        foreach ($result as $data) {
            $found[$data['key']] = $data['value'];
        }
        $this->assertEquals(10, $found[10]);
        $this->assertEquals(20, $found[20]);
    }

    public function testGeneratorInput(): void
    {
        $generator = function () {
            yield 'a' => 1;
            yield 'b' => 2;
            yield 'c' => 3;
        };

        $result = $this->generatorToArray(
            (new MapReduceBuilder())
                ->input($generator())
                ->map(fn($v) => yield ['sum', $v])
                ->reduce(function($k, $vIterator) {
                    $sum = 0;
                    foreach ($vIterator as $value) {
                        $sum += $value;
                    }
                    return $sum;
                })
                ->execute()
        );

        $this->assertEquals(6, $result['sum']['value']);
    }

    public function testSplFileObjectInput(): void
    {
        $file = "{$this->tempDir}/data.txt";
        file_put_contents($file, "line1\nline2\nline3");

        $result = $this->generatorToArray(
            (new MapReduceBuilder())
                ->input(new \SplFileObject($file))
                ->map(function ($line) {
                    $line = trim($line);
                    if (!empty($line)) {
                        yield ['count', 1];
                    }
                })
                ->reduce(function($k, $vIterator) {
                    $sum = 0;
                    foreach ($vIterator as $value) {
                        $sum += $value;
                    }
                    return $sum;
                })
                ->execute()
        );

        $this->assertEquals(3, $result['count']['value']);
    }

    public function testCsvInput(): void
    {
        $file = "{$this->tempDir}/data.csv";
        file_put_contents($file, "name,value\nAlice,100\nBob,200");

        $csvGenerator = function () use ($file) {
            $handle = fopen($file, 'r');
            if ($handle === false) {
                return;
            }
            fgetcsv($handle); // Skip header
            while (($row = fgetcsv($handle)) !== false) {
                yield ['name' => $row[0], 'value' => (int)$row[1]];
            }
            fclose($handle);
        };

        $result = $this->generatorToArray(
            (new MapReduceBuilder())
                ->input($csvGenerator())
                ->map(fn($row) => yield [$row['name'], $row['value']])
                ->reduce(fn($k, $vIterator) => iterator_to_array($vIterator)[0])
                ->execute()
        );

        $this->assertEquals(100, $result['Alice']['value']);
        $this->assertEquals(200, $result['Bob']['value']);
    }

    public function testJsonLinesInput(): void
    {
        $file = "{$this->tempDir}/data.jsonl";
        file_put_contents($file, json_encode(['id' => 1, 'name' => 'Alice']) . "\n");
        file_put_contents($file, json_encode(['id' => 2, 'name' => 'Bob']) . "\n", FILE_APPEND);

        $jsonlGenerator = function () use ($file) {
            $handle = fopen($file, 'r');
            if ($handle === false) {
                return;
            }
            while (($line = fgets($handle)) !== false) {
                yield json_decode(trim($line), true);
            }
            fclose($handle);
        };

        $result = $this->generatorToArray(
            (new MapReduceBuilder())
                ->input($jsonlGenerator())
                ->map(fn($row) => yield [$row['name'], $row['id']])
                ->reduce(fn($k, $vIterator) => iterator_to_array($vIterator)[0])
                ->execute()
        );

        $this->assertEquals(1, $result['Alice']['value']);
        $this->assertEquals(2, $result['Bob']['value']);
    }

    public function testDirectoryScanningInput(): void
    {
        $dir = "{$this->tempDir}/logs";
        mkdir($dir);
        file_put_contents("$dir/file1.log", "error");
        file_put_contents("$dir/file2.log", "error");
        file_put_contents("$dir/file3.txt", "skip");

        $fileGenerator = function () use ($dir) {
            $iterator = new \RecursiveIteratorIterator(
                new \RecursiveDirectoryIterator($dir)
            );
            foreach ($iterator as $file) {
                if ($file->isFile() && $file->getExtension() === 'log') {
                    yield $file->getPathname();
                }
            }
        };

        $result = $this->generatorToArray(
            (new MapReduceBuilder())
                ->input($fileGenerator())
                ->map(fn($path) => yield ['count', 1])
                ->reduce(function($k, $vIterator) {
                    $sum = 0;
                    foreach ($vIterator as $value) {
                        $sum += $value;
                    }
                    return $sum;
                })
                ->execute()
        );

        $this->assertEquals(2, $result['count']['value']);
    }
}
