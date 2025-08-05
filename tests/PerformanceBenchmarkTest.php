<?php

namespace MHz\MysqlVector\Tests;

use MHz\MysqlVector\VectorTable;

class PerformanceBenchmarkTest extends BaseVectorTest
{
    private $dimension = 384;
    private $testVectorAmount = 1000;

    public function testSearchPerformance() {
        $vectorTable = $this->makeTable('search_performance_test', $this->dimension);
        $vectorTable->getConnection()->begin_transaction();

        // Let's insert a known vector
        $targetVector = array_fill(0, $this->dimension, 0.5);
        $vectorTable->upsert($targetVector);

        $vecs = $this->getRandomVectors(100, $this->dimension);
        $vectorTable->batchInsert($vecs);

        // Now, we search for this vector
        echo "Searching for 1 vector among 100...\n";
        $time = microtime(true);
        $results = $vectorTable->search($targetVector);
        $time = microtime(true) - $time;
        echo sprintf("Search completed in %.2f seconds\n", $time);

        $vecs = $this->getRandomVectors(900, $this->dimension);
        $vectorTable->batchInsert($vecs);

        // Now, we search for this vector
        echo "Searching for 1 vector among 1000...\n";
        $time = microtime(true);
        $results = $vectorTable->search($targetVector);
        $time = microtime(true) - $time;
        echo sprintf("Search completed in %.2f seconds\n", $time);

        $vecs = $this->getRandomVectors(9000, $this->dimension);
        $vectorTable->batchInsert($vecs);

        // Now, we search for this vector
        echo "Searching for 1 vector among 10000...\n";
        $time = microtime(true);
        $results = $vectorTable->search($targetVector, 10);
        $time = microtime(true) - $time;
        echo sprintf("Search completed in %.2f seconds\n", $time);

        $vecs = $this->getRandomVectors(90000, $this->dimension);
        $vectorTable->batchInsert($vecs);

        // Now, we search for this vector
        echo "Searching for 1 vector among 100000...\n";
        $time = microtime(true);
        $results = $vectorTable->search($targetVector, 10);
        $time = microtime(true) - $time;
        echo sprintf("Search completed in %.2f seconds\n", $time);

        $vecs = $this->getRandomVectors(900000, $this->dimension);
        $vectorTable->batchInsert($vecs);

        // Now, we search for this vector
        echo "Searching for 1 vector among 1000000...\n";
        $time = microtime(true);
        $results = $vectorTable->search($targetVector, 10);
        $time = microtime(true) - $time;
        echo sprintf("Search completed in %.2f seconds\n", $time);

        $vecs = $this->getRandomVectors(9000000, $this->dimension);
        $vectorTable->batchInsert($vecs);

        // Now, we search for this vector
        echo "Searching for 1 vector among 10000000...\n";
        $time = microtime(true);
        $results = $vectorTable->search($targetVector, 10);
        $time = microtime(true) - $time;
        echo sprintf("Search completed in %.2f seconds\n", $time);

        $vectorTable->getConnection()->rollback();
    }

    private function getRandomVectors($count, $dimension) {
        $vecs = [];
        for ($i = 0; $i < $count; $i++) {
            for($j = 0; $j < $dimension; $j++) {
                $vecs[$i][$j] = 2 * (mt_rand(0, 1000) / 1000) - 1;
            }
        }
        return $vecs;
    }


}
