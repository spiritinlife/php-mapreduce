<?php

declare(strict_types=1);

namespace Spiritinlife\MapReduce\Utils;

/**
 * Min-heap for efficient k-way merge in shuffle phase
 *
 * Compares records by their serialized keys for optimal merge performance.
 * This reduces the time complexity of finding the minimum from O(k) to O(log k)
 * per operation, where k is the number of chunks being merged.
 *
 * @extends \SplMinHeap<array{record: array{serialized_key: string, original_key: mixed, value: mixed}, fileIndex: int}>
 */
class MergeMinHeap extends \SplMinHeap
{
    /**
     * Compare two heap elements
     *
     * SplMinHeap extracts elements with HIGHER compare values first (top of heap).
     * For ascending key order, we want smaller keys extracted first, so we REVERSE the comparison.
     *
     * @param array{record: array{serialized_key: string, original_key: mixed, value: mixed}, fileIndex: int} $a Element with 'record' and 'fileIndex' keys
     * @param array{record: array{serialized_key: string, original_key: mixed, value: mixed}, fileIndex: int} $b Element with 'record' and 'fileIndex' keys
     * @return int Positive if priority($a) > priority($b), negative otherwise
     */
    protected function compare($a, $b): int
    {
        // Primary comparison: by serialized key (reversed for min-heap)
        $cmp = strcmp($b['record']['serialized_key'], $a['record']['serialized_key']);

        // If keys are equal, use file index as tie-breaker for stable sorting
        // Lower file indices should be extracted first, so reverse comparison
        if ($cmp === 0) {
            return $b['fileIndex'] <=> $a['fileIndex'];
        }

        return $cmp;
    }
}
