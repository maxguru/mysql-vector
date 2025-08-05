<?php

namespace MHz\MysqlVector\Tests;

use MHz\MysqlVector\VectorTable;

class PerformanceBenchmarkTest extends BaseVectorTest
{
    public function testSearchPerformance() {
        echo "=== Memory-Efficient Performance Benchmark ===\n";
        echo "Initial memory: " . $this->getMemoryUsage() . "\n\n";

        // Create VectorTable for performance testing
        $vectorTable = $this->makeTable('performance_test', 384);
        self::$mysqli->begin_transaction();

        // Insert a known target vector
        $targetVector = array_fill(0, 384, 0.5);
        $vectorTable->upsert($targetVector);

        // Test with progressively larger datasets using chunked insertion
        $testSizes = [100, 1000, 10000, 100000];
        $currentTotal = 1; // We already have the target vector

        foreach ($testSizes as $targetSize) {
            $toInsert = $targetSize - $currentTotal;
            if ($toInsert > 0) {
                echo "Preparing dataset of $targetSize vectors...\n";
                $this->insertRandomVectorsChunked($vectorTable, $toInsert, 384, 1000);
                $currentTotal = $targetSize;
            }

            // Perform search test
            echo "Searching for 1 vector among $targetSize...\n";
            $time = microtime(true);
            $results = $vectorTable->search($targetVector, 10);
            $time = microtime(true) - $time;
            echo sprintf("Search completed in %.4f seconds (Memory: %s)\n", $time, $this->getMemoryUsage());

            // Performance validations with assertions
            $this->assertGreaterThan(0, count($results), "Search should return results for dataset size $targetSize");
            $this->assertLessThan(5.0, $time, "Search took too long: {$time}s for $targetSize vectors");
            $this->assertGreaterThan(0.0, $results[0]['similarity'], "Top result should have positive similarity");

            // Memory efficiency validation
            $currentMemory = memory_get_usage(true) / 1024 / 1024; // MB
            $this->assertLessThan(100, $currentMemory, "Memory usage too high: {$currentMemory}MB");

            // Verify we got results
            if (!empty($results)) {
                echo "✓ Found " . count($results) . " results, top similarity: " .
                     number_format($results[0]['similarity'], 4) . "\n";
            }
            echo "\n";

            // Force garbage collection between tests
            gc_collect_cycles();
        }

        echo "=== Performance test completed ===\n";
        echo "Final memory usage: " . $this->getMemoryUsage() . "\n";

        // Final assertion to ensure test is not marked as risky
        $this->assertEquals(1, 1, "Performance benchmark completed successfully");

        // Rollback transaction to clean up test data
        self::$mysqli->rollback();
    }

    /**
     * Memory-efficient chunked vector insertion
     * @param int $count Total number of vectors to insert
     * @param int $dimension Vector dimension
     * @param int $chunkSize Number of vectors per chunk (default: 1000)
     */
    private function insertRandomVectorsChunked(VectorTable $vectorTable, $count, $dimension, $chunkSize = 1000) {
        $inserted = 0;
        echo "  Inserting $count vectors in chunks of $chunkSize...\n";

        while ($inserted < $count) {
            $remaining = min($chunkSize, $count - $inserted);

            // Generate chunk of random vectors inline
            $chunk = [];
            for ($i = 0; $i < $remaining; $i++) {
                $vector = [];
                for($j = 0; $j < $dimension; $j++) {
                    $vector[$j] = 2 * (mt_rand(0, 1000) / 1000) - 1;
                }
                $chunk[] = $vector;
            }

            echo "    Processing chunk: " . ($inserted + 1) . "-" . ($inserted + $remaining) . "/$count\n";
            $vectorTable->batchInsert($chunk); // Efficient batch insert with automatic graph building
            $inserted += $remaining;

            // Force garbage collection and memory cleanup
            unset($chunk);
            gc_collect_cycles();

            if ($inserted % 10000 === 0) {
                echo "    Inserted $inserted/$count vectors (Memory: " . $this->getMemoryUsage() . ")\n";
            }
        }
        echo "  ✓ Completed inserting $count vectors (including graph building)\n";
    }

    /**
     * Get current memory usage in human-readable format
     */
    private function getMemoryUsage(): string
    {
        $bytes = memory_get_usage(true);
        $units = ['B', 'KB', 'MB', 'GB'];
        $i = 0;
        while ($bytes >= 1024 && $i < 3) {
            $bytes /= 1024;
            $i++;
        }
        return round($bytes, 2) . ' ' . $units[$i];
    }
}
