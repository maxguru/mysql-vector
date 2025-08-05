<?php

namespace MHz\MysqlVector\Tests;

use PHPUnit\Framework\TestCase;
use MHz\MysqlVector\VectorTable;

/**
 * Base test class for MySQL Vector library tests
 * Provides common functionality for database setup, cleanup, and VectorTable management
 */
abstract class BaseVectorTest extends TestCase
{
    /**
     * Database connection parameters
     */
    protected const DB_HOST = 'db';
    protected const DB_USERNAME = 'db';
    protected const DB_PASSWORD = 'db';
    protected const DB_DATABASE = 'db';
    protected const DB_PORT = 3306;

    /**
     * Shared database connection for all VectorTable instances
     */
    protected static ?\mysqli $mysqli = null;

    /**
     * Array of VectorTable instances created during testing
     * Indexed by table name for easy access
     */
    protected array $vectorTables = [];

    /**
     * Defensive cleanup for interrupted tests
     * @param \mysqli $connection Database connection
     * @param string $tableName Base table name for VectorTable
     * @param int $dimension Vector dimension for table name generation
     */
    protected function cleanupDatabaseArtifacts(\mysqli $connection, string $tableName, int $dimension): void
    {
        // Create temporary VectorTable instance for defensive cleanup
        $tempVectorTable = new VectorTable($connection, $tableName, $dimension);

        try {
            $tempVectorTable->deinitializeTables();
        } catch (\Exception $e) {
            // Ignore errors during defensive cleanup
        }
    }

    /**
     * Set up database connection before all tests in the class
     */
    public static function setUpBeforeClass(): void
    {
        self::$mysqli = new \mysqli(
            self::DB_HOST,
            self::DB_USERNAME,
            self::DB_PASSWORD,
            self::DB_DATABASE,
            self::DB_PORT
        );

        // Check connection
        if (self::$mysqli->connect_error) {
            throw new \Exception("Database connection failed: " . self::$mysqli->connect_error);
        }

        // Initialize global MySQL functions once for all tests
        VectorTable::initializeFunctions(self::$mysqli);
    }

    /**
     * Set up test environment
     * Child classes can override this method and call parent::setUp() to extend functionality
     */
    protected function setUp(): void
    {
        // Initialize empty array for VectorTable instances
        $this->vectorTables = [];
    }

    /**
     * Create a new VectorTable instance with automatic cleanup
     * @param string $tableNamePrefix Prefix for the table name (will be made unique)
     * @param int $dimension Vector dimension (defaults to 384)
     * @return VectorTable The created and initialized VectorTable instance
     */
    protected function makeTable(string $tableNamePrefix, int $dimension = 384): VectorTable
    {
        // Create unique table name to avoid conflicts
        $uniqueTableName = $tableNamePrefix . '_' . uniqid();

        // Defensive cleanup in case previous test was interrupted
        $this->cleanupDatabaseArtifacts(self::$mysqli, $uniqueTableName, $dimension);

        // Create new VectorTable instance and initialize only tables (functions already initialized)
        $vectorTable = new VectorTable(self::$mysqli, $uniqueTableName, $dimension);
        $vectorTable->initializeTables();

        // Store in array for automatic cleanup
        $this->vectorTables[$uniqueTableName] = $vectorTable;

        return $vectorTable;
    }

    /**
     * Clean up after each test
     * Performs cleanup for all created VectorTable instances
     * Child classes can override this method and call parent::tearDown() to extend functionality
     */
    protected function tearDown(): void
    {
        // Clean up all VectorTable instances created during the test
        foreach ($this->vectorTables as $vectorTable) {
            if ($vectorTable) {
                try {
                    $vectorTable->deinitializeTables();
                } catch (\Exception $e) {
                    // Log error but continue cleanup
                    error_log("Failed to cleanup VectorTable: " . $e->getMessage());
                }
            }
        }

        // Clear the array
        $this->vectorTables = [];
    }

    /**
     * Clean up database connection after all tests in the class
     */
    public static function tearDownAfterClass(): void
    {
        if (self::$mysqli && !self::$mysqli->connect_error) {
            // Clean up global MySQL functions
            try {
                VectorTable::deinitializeFunctions(self::$mysqli);
            } catch (\Exception $e) {
                // Log error but continue cleanup
                error_log("Failed to cleanup MySQL functions: " . $e->getMessage());
            }

            self::$mysqli->close();
        }
        self::$mysqli = null;
    }
}
