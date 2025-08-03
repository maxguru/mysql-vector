<?php

namespace MHz\MysqlVector\Tests;

use MHz\MysqlVector\VectorTable;
use PHPUnit\Framework\TestCase;

class PerformanceBenchmarkTest extends TestCase
{
    private $vectorTable;
    private $dimension = 384;
    private $testVectorAmount = 1000;

    protected function setUp(): void
    {
        $mysqli = new \mysqli('db', 'db', 'db', 'db', 3306);

        // Check connection
        if ($mysqli->connect_error) {
            die("Connection failed: " . $mysqli->connect_error);
        }

        // Setup VectorTable for testing
        $this->vectorTable = new VectorTable($mysqli, 'test_table', $this->dimension);

        // Create required tables for testing
        $this->vectorTable->initialize();
    }

    public function testSearchPerformance() {
        $this->vectorTable->getConnection()->begin_transaction();

        // Let's insert a known vector
        $targetVector = array_fill(0, $this->dimension, 0.5);
        $this->vectorTable->upsert($targetVector);

        $vecs = $this->getRandomVectors(100, $this->dimension);
        $this->vectorTable->batchInsert($vecs);

        // Now, we search for this vector
        echo "Searching for 1 vector among 100...\n";
        $time = microtime(true);
        $results = $this->vectorTable->search($targetVector);
        $time = microtime(true) - $time;
        echo sprintf("Search completed in %.2f seconds\n", $time);

        $vecs = $this->getRandomVectors(900, $this->dimension);
        $this->vectorTable->batchInsert($vecs);

        // Now, we search for this vector
        echo "Searching for 1 vector among 1000...\n";
        $time = microtime(true);
        $results = $this->vectorTable->search($targetVector);
        $time = microtime(true) - $time;
        echo sprintf("Search completed in %.2f seconds\n", $time);

        $vecs = $this->getRandomVectors(9000, $this->dimension);
        $this->vectorTable->batchInsert($vecs);

        // Now, we search for this vector
        echo "Searching for 1 vector among 10000...\n";
        $time = microtime(true);
        $results = $this->vectorTable->search($targetVector, 10);
        $time = microtime(true) - $time;
        echo sprintf("Search completed in %.2f seconds\n", $time);

        $vecs = $this->getRandomVectors(90000, $this->dimension);
        $this->vectorTable->batchInsert($vecs);

        // Now, we search for this vector
        echo "Searching for 1 vector among 100000...\n";
        $time = microtime(true);
        $results = $this->vectorTable->search($targetVector, 10);
        $time = microtime(true) - $time;
        echo sprintf("Search completed in %.2f seconds\n", $time);

        $vecs = $this->getRandomVectors(900000, $this->dimension);
        $this->vectorTable->batchInsert($vecs);

        // Now, we search for this vector
        echo "Searching for 1 vector among 1000000...\n";
        $time = microtime(true);
        $results = $this->vectorTable->search($targetVector, 10);
        $time = microtime(true) - $time;
        echo sprintf("Search completed in %.2f seconds\n", $time);

        $vecs = $this->getRandomVectors(9000000, $this->dimension);
        $this->vectorTable->batchInsert($vecs);

        // Now, we search for this vector
        echo "Searching for 1 vector among 10000000...\n";
        $time = microtime(true);
        $results = $this->vectorTable->search($targetVector, 10);
        $time = microtime(true) - $time;
        echo sprintf("Search completed in %.2f seconds\n", $time);

        $this->vectorTable->getConnection()->rollback();
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

    public static function tearDownAfterClass(): void
    {
        // Clean up the database and close connection
        $mysqli = new \mysqli('db', 'db', 'db', 'db', 3306);
        $vectorTable = new VectorTable($mysqli, 'test_table', 3);
        $mysqli->query("DROP TABLE IF EXISTS " . $vectorTable->getVectorTableName());
        $mysqli->query("DROP FUNCTION IF EXISTS COSIM");
        $mysqli->close();
    }

    protected function tearDown(): void
    {
        // Clean up the database and close connection
        $this->vectorTable->getConnection()->query("DROP TABLE IF EXISTS" . $this->vectorTable->getVectorTableName());
        $this->vectorTable->getConnection()->query("DROP FUNCTION IF EXISTS COSIM");
        $this->vectorTable->getConnection()->close();
    }
}
