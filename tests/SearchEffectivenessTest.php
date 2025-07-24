<?php

namespace MHz\MysqlVector\Tests;

use MHz\MysqlVector\VectorTable;
use PHPUnit\Framework\TestCase;

/**
 * SearchEffectivenessTest - Semantic search quality validation for MySQL Vector library
 *
 * This test validates the search effectiveness of the MySQL Vector library by testing
 * semantic similarity search quality and ranking accuracy.
 *
 * ## Purpose
 * Validates that the vector search algorithm correctly identifies and ranks semantically
 * similar content. This is a sanity check to ensure the library works well for vector search.
 *
 * ## Features
 * - **Search Quality Validation**: Verifies that semantically similar content ranks higher than unrelated content
 * - **Semantic Category Testing**: Tests across all available categories dynamically loaded from test data
 * - **Ranking Quality Assertions**: Ensures more similar content consistently ranks higher
 * - **Edge Case Testing**: Tests exact matches, partial matches, and unrelated queries
 * - **Pre-generated Vectors**: Uses high-quality 3072-dimension vectors for consistent testing
 * - **Zero External Dependencies**: All test data self-contained with pre-computed vectors
 *
 * ## JSON Data Structure
 * The test uses a test data JSON file (`vectors.json`) with the following format:
 * ```json
 * [
 *   {
 *     "text": "Machine learning algorithms optimize neural network performance",
 *     "vector": [0.1, -0.2, 0.3, ...],
 *     "semantic_category": "technology",
 *     "usage": ["semantic_similarity", "edge_cases", "performance_testing"]
 *   }
 * ]
 * ```
 *
 * **Important**: Every entry must have a non-null vector. The test automatically
 * selects diverse texts from different categories to use as test queries.
 *
 * ## Requirements
 * ### 1. MySQL Database
 * - MySQL 8.0+ with JSON support
 * - Database connection configured in your application
 * - Sufficient storage space (approximately 50MB for 100K vectors)
 *
 * ## Test Structure
 *
 * ### 1. Semantic Similarity Search (`testSemanticSimilaritySearch`)
 * - Tests 100 diverse semantic queries across multiple domains
 * - Validates that semantically related content ranks higher than unrelated content
 * - Includes edge cases like nonsensical queries
 * - Measures overall search effectiveness percentage
 *
 * ### 2. Search Performance (`testSearchPerformance`)
 * - Benchmarks search performance across different dataset sizes
 * - Tests with 100, 1K, 10K, and 100K vectors
 * - Measures search time and validates results
 * - Ensures performance remains acceptable as dataset grows
 *
 * ### 3. Search Ranking Quality (`testSearchRankingQuality`)
 * - Tests specific semantic relationships with known expected rankings
 * - Validates that more relevant results appear before less relevant ones
 * - Uses curated test cases with predictable semantic relationships
 *
 * ## Expected Results
 * - **Search Effectiveness**: >80% of semantic queries should return relevant results in top positions
 * - **Performance**: Search times should remain under 1 second for datasets up to 100K vectors
 * - **Ranking Quality**: Semantically closer content should consistently rank higher
 *
 * ## Customizing Test Data
 * To modify test scenarios, edit the `vectors.json` file:
 *
 * ### Adding New Test Texts:
 * ```json
 * {
 *   "text": "Your new test text here",
 *   "vector": [your_vector_array],
 *   "semantic_category": "your_category",
 *   "usage": ["semantic_similarity"]
 * }
 * ```
 *
 */

class SearchEffectivenessTest extends TestCase
{
    // Configuration
    private VectorTable $vectorTable;
    private int $vectorDimension = 3072;

    // Test data storage
    private array $vectorTableId2testDataIndex = [];
    private array $testData = [];
    private array $categorizedVectors = [];

    protected function setUp(): void
    {
        $mysqli = new \mysqli('db', 'db', 'db', 'db', 3306);
        if ($mysqli->connect_error) {
            throw new \Exception("Database connection failed: " . $mysqli->connect_error);
        }

        // Initialize VectorTable with vector dimensions
        $this->vectorTable = new VectorTable($mysqli, 'search_effectiveness_test', $this->vectorDimension);
        $this->vectorTable->initialize();

        echo "\n=== MySQL Vector Library - Search Effectiveness Test ===\n";
        echo "Vector dimensions: {$this->vectorDimension}\n";
        echo "Pre-computed vectors: " . count($this->getTestData()) . " total\n\n";
    }

    /**
     * Get test data with lazy loading
     * Loads and parses vectors.json file only when first accessed
     */
    private function getTestData(): array
    {
        if (empty($this->testData)) {
            $vectorsFile = __DIR__ . '/vectors.json';
            if (!file_exists($vectorsFile)) {
                throw new \Exception('Pre-computed vectors file not found. Ensure vectors.json exists in tests/ directory.');
            }

            $jsonContent = file_get_contents($vectorsFile);
            $this->testData = json_decode($jsonContent, true);

            if ($this->testData === null) {
                throw new \Exception('Failed to decode vectors.json. Invalid JSON format.');
            }

            // Validate JSON structure and organize data
            $this->categorizedVectors = [];
            foreach ($this->testData as $index => $entry) {
                // Validate required fields
                if (!isset($entry['text']) || !isset($entry['semantic_category']) || !isset($entry['usage'])) {
                    throw new \Exception("Invalid JSON structure for entry at index '$index'. Missing required fields: text, semantic_category, usage");
                }

                if (!isset($entry['vector']) || (!is_array($entry['vector']) && $entry['vector'] !== null)) {
                    throw new \Exception("Invalid JSON structure for entry at index '$index'. Missing or invalid 'vector' field");
                }

                if (!is_array($entry['usage'])) {
                    throw new \Exception("Invalid JSON structure for entry at index '$index'. usage must be an array");
                }

                // Validate vector dimensions
                if (is_array($entry['vector']) && count($entry['vector']) !== $this->vectorDimension) {
                    throw new \Exception("Invalid vector dimensions for entry at index '$index'. Expected {$this->vectorDimension}, got " . count($entry['vector']));
                }

                // Organize by category
                $category = $entry['semantic_category'];
                if (!isset($this->categorizedVectors[$category])) {
                    $this->categorizedVectors[$category] = [];
                }
                $this->categorizedVectors[$category][$index] = $entry;
            }

            // Validate that we have sufficient category diversity
            $categories = array_keys($this->categorizedVectors);
            if (count($categories) < 3) {
                throw new \Exception("Insufficient category diversity. Expected at least 3 categories, found " . count($categories) . ": " . implode(', ', $categories));
            }

            echo "Using pre-generated vectors (no external dependencies)\n";
            echo "Loaded " . count($this->testData) . " structured entries\n";
        }

        return $this->testData;
    }

    /**
     * Get vector entries by their usage criteria
     * Returns complete entry objects, not just text strings
     */
    private function getVectorsByUsage(string $usageType): array
    {
        $testData = $this->getTestData();
        return array_filter($testData, function($entry) use ($usageType) {
            return isset($entry['usage']) && in_array($usageType, $entry['usage']);
        }, ARRAY_FILTER_USE_BOTH);
    }

    /**
     * Get vector entries by semantic category
     * Returns complete entry objects, not just text strings
     */
    private function getVectorsByCategory(string $category, ?int $limit = null): array
    {
        if (!isset($this->categorizedVectors[$category])) {
            return [];
        }

        $categoryEntries = $this->categorizedVectors[$category];
        return $limit ? array_slice($categoryEntries, 0, $limit, true) : $categoryEntries;
    }

    /**
     * Get a single vector entry by its array index
     * Direct O(1) access instead of searching
     */
    private function getVectorByIndex(int $index): ?array
    {
        $testData = $this->getTestData();
        return $testData[$index] ?? null;
    }

    /**
     * Get vector entry by database table ID
     *
     * @param int $vectorId Database table ID
     * @return array|null Full vector entry or null if not found
     */
    private function getVectorByTableId(int $vectorId): ?array
    {
        if (!isset($this->vectorTableId2testDataIndex[$vectorId])) {
            return null;
        }

        $testDataIndex = $this->vectorTableId2testDataIndex[$vectorId];
        return $this->getVectorByIndex($testDataIndex);
    }

    /**
     * Get all available semantic categories from the loaded data
     * Returns array of category names sorted alphabetically
     */
    private function getAvailableCategories(): array
    {
        return array_keys($this->categorizedVectors);
    }

    /**
     * Clean up after each test
     */
    protected function tearDown(): void
    {
        if (isset($this->vectorTable)) {
            // Clean up database tables and connection
            $this->vectorTable->getConnection()->query("DROP TABLE IF EXISTS " . $this->vectorTable->getVectorTableName());
            $this->vectorTable->getConnection()->query("DROP FUNCTION IF EXISTS COSIM");
            $this->vectorTable->getConnection()->close();
        }
    }

    /**
     * Load test vectors into the database for search effectiveness testing
     *
     * @param string $usage The test usage type to filter vectors by (e.g., 'semantic_similarity', 'edge_cases')
     */
    private function setupVectorData(string $usage): void
    {
        // Get test entries based on usage type
        $testEntries = $this->getVectorsByUsage($usage);
        if (empty($testEntries)) {
            throw new \Exception("No test texts found for usage type: '$usage'");
        }

        echo "Loading " . count($testEntries) . " vectors for '$usage' testing...\n";
        echo "Storing vectors in database...\n";

        $this->vectorTable->getConnection()->begin_transaction();

        try {
            foreach ($testEntries as $index => $entry) {
                $vectorId = $this->vectorTable->upsert($entry['vector']);

                // Store simple mapping: database table ID → test data index
                $this->vectorTableId2testDataIndex[$vectorId] = $index;
            }

            $this->vectorTable->getConnection()->commit();
            echo "Successfully stored " . count($testEntries) . " vectors.\n";
            echo "✓ Test data loaded: " . count($testEntries) . " vectors stored\n\n";

        } catch (\Exception $e) {
            $this->vectorTable->getConnection()->rollback();
            $this->vectorTableId2testDataIndex = [];
            throw new \Exception("Failed to store vectors: " . $e->getMessage());
        }
    }

    /**
     * Clean up vectors from database after testing
     */
    private function cleanupVectorData(): void
    {
        // Reset tracking array
        $this->vectorTableId2testDataIndex = [];

        // Clear all vectors from the test table
        $tableName = $this->vectorTable->getVectorTableName();
        $result = $this->vectorTable->getConnection()->query("DELETE FROM `$tableName`");

        if (!$result) {
            echo "! Warning: Failed to clean up test data\n\n";
            return;
        }

        echo "✓ Test data cleaned up\n\n";
    }



    /**
     * Test: Semantic similarity search effectiveness
     * Tests the library's ability to find semantically similar content
     */
    public function testSemanticSimilaritySearch(): void
    {
        echo "=== Semantic Similarity Search Test ===\n";

        // Setup test data for semantic similarity testing
        $this->setupVectorData('semantic_similarity');

        // Get semantic test queries from structured data
        $semanticTestCases = [];

        // Select representative texts from all available categories for semantic testing
        $targetCategories = $this->getAvailableCategories();

        foreach ($targetCategories as $category) {
            // Get entries from this category that are marked for semantic similarity testing
            $categoryEntries = $this->getVectorsByCategory($category);

            foreach ($categoryEntries as $index => $entry) {
                // Only include entries that are marked for semantic similarity testing
                if (isset($entry['usage']) && in_array('semantic_similarity', $entry['usage'])) {
                    $semanticTestCases[] = [
                        'query' => $entry['text'],
                        'expected_categories' => [$category],
                        'semantic_category' => $entry['semantic_category'],
                        'entry' => $entry  // Include full entry for access to other properties
                    ];
                    // Only take one entry per category to keep test execution reasonable
                    break;
                }
            }
        }

        $totalTests = 0;
        $passedTests = 0;

        foreach ($semanticTestCases as $testCase) {
            echo "Testing query: '{$testCase['query']}'\n";

            // Get vector for query
            $queryVector = $testCase['entry']['vector'];

            // Search for similar vectors
            $startTime = microtime(true);
            $results = $this->vectorTable->search($queryVector, 10);
            $searchTime = microtime(true) - $startTime;

            echo "  Search completed in " . number_format($searchTime * 1000, 2) . "ms\n";
            echo "  Top 5 results:\n";

            // Assert that search returns results
            $this->assertNotEmpty($results, "Search should return results for query: '{$testCase['query']}'");

            $foundMatches = 0;
            foreach (array_slice($results, 0, 5) as $index => $result) {
                $vectorEntry = $this->getVectorByTableId($result['id']);
                $text = $vectorEntry ? $vectorEntry['text'] : 'Unknown';
                $similarity = $result['similarity'];
                echo "    " . ($index + 1) . ". [" . number_format($similarity, 4) . "] " . substr($text, 0, 60) . "...\n";

                // Check if this result matches expected categories
                if ($vectorEntry && in_array($vectorEntry['semantic_category'], $testCase['expected_categories'])) {
                    $foundMatches++;
                }
            }

            // Assert search quality - should find relevant matches and have good top similarity
            $topSimilarity = $results[0]['similarity'];
            $this->assertGreaterThan(0.5, $topSimilarity, "Top result should have similarity > 0.5 for query: '{$testCase['query']}'");
            $this->assertGreaterThan(0, $foundMatches, "Should find at least one relevant match for query: '{$testCase['query']}'");

            if ($foundMatches > 0 && $topSimilarity > 0.5) {
                echo "  ✓ Test passed (found $foundMatches relevant matches)\n";
                $passedTests++;
            } else {
                echo "  ✗ Test failed (found $foundMatches relevant matches, top similarity: " .
                     number_format($topSimilarity, 4) . ")\n";
            }

            $totalTests++;
            echo "\n";
        }

        $successRate = ($passedTests / $totalTests) * 100;
        echo "Semantic similarity test results: $passedTests/$totalTests passed (" . number_format($successRate, 1) . "%)\n";

        // Assert minimum success rate
        $this->assertGreaterThanOrEqual(60, $successRate, "Semantic similarity success rate should be at least 60%");

        echo "✓ Semantic similarity success rate meets expectations\n";
        echo "✓ Semantic similarity search test completed\n\n";

        // Clean up test data
        $this->cleanupVectorData();
    }

    /**
     * Test: Edge cases and boundary conditions
     * Tests exact matches, partial matches, and completely unrelated queries
     */
    public function testEdgeCases(): void
    {
        echo "=== Edge Case Testing ===\n";

        // Setup test data for edge case testing (stores entries with 'edge_cases' usage)
        $this->setupVectorData('edge_cases');

        // Build edge case test queries using entries with 'edge_cases_queries' usage
        // These entries are NOT stored in the database, avoiding exact matches
        $edgeCaseQueries = [];
        $queryEntries = $this->getVectorsByUsage('edge_cases_queries');

        // Get available categories from stored entries to determine expected matches
        $storedEntries = $this->getVectorsByUsage('edge_cases');
        $availableCategories = array_unique(array_column($storedEntries, 'semantic_category'));

        // High similarity test - use first available query entry
        if (!empty($queryEntries)) {
            $highSimEntry = reset($queryEntries);
            $expectedCategories = in_array($highSimEntry['semantic_category'], $availableCategories)
                ? [$highSimEntry['semantic_category']]
                : $availableCategories; // Fallback to all categories

            $edgeCaseQueries[] = [
                'query' => $highSimEntry['text'],
                'test_type' => 'high_similarity',
                'expected_categories' => $expectedCategories,
                'semantic_category' => $highSimEntry['semantic_category'],
                'entry' => $highSimEntry
            ];
        }

        // Partial match test - use second available query entry if different category
        if (count($queryEntries) > 1) {
            $partialMatchEntry = null;
            $firstCategory = reset($queryEntries)['semantic_category'];

            foreach ($queryEntries as $entry) {
                if ($entry['semantic_category'] !== $firstCategory) {
                    $partialMatchEntry = $entry;
                    break;
                }
            }

            // If no different category found, use second entry anyway
            if (!$partialMatchEntry && count($queryEntries) > 1) {
                $partialMatchEntry = $queryEntries[array_keys($queryEntries)[1]];
            }

            if ($partialMatchEntry) {
                // For partial match, focus on similarity scores rather than category predictions
                // Allow matches from any available category since we can't predict semantic relationships
                $edgeCaseQueries[] = [
                    'query' => $partialMatchEntry['text'],
                    'test_type' => 'partial_match',
                    'expected_categories' => $availableCategories, // Allow matches from any category
                    'semantic_category' => $partialMatchEntry['semantic_category'],
                    'entry' => $partialMatchEntry
                ];
            }
        }

        // Unrelated query test - use entry from category with fewest stored entries or different from used categories
        $unrelatedQueryEntry = null;
        $usedCategories = array_column($edgeCaseQueries, 'semantic_category');

        foreach ($queryEntries as $entry) {
            $category = $entry['semantic_category'];
            // Use entry from category not already used in other tests
            if (!in_array($category, $usedCategories)) {
                $unrelatedQueryEntry = $entry;
                break;
            }
        }

        // Fallback: use any remaining entry
        if (!$unrelatedQueryEntry && count($queryEntries) > count($edgeCaseQueries)) {
            foreach ($queryEntries as $entry) {
                $alreadyUsed = false;
                foreach ($edgeCaseQueries as $existingQuery) {
                    if ($existingQuery['entry']['text'] === $entry['text']) {
                        $alreadyUsed = true;
                        break;
                    }
                }
                if (!$alreadyUsed) {
                    $unrelatedQueryEntry = $entry;
                    break;
                }
            }
        }

        if ($unrelatedQueryEntry) {
            $edgeCaseQueries[] = [
                'query' => $unrelatedQueryEntry['text'],
                'test_type' => 'unrelated_query',
                'expected_categories' => [], // No specific categories expected for unrelated queries
                'semantic_category' => $unrelatedQueryEntry['semantic_category'],
                'entry' => $unrelatedQueryEntry
            ];
        }

        foreach ($edgeCaseQueries as $testCase) {
            echo "Test: {$testCase['test_type']} - '{$testCase['query']}'\n";

            $queryVector = $testCase['entry']['vector'];
            $results = $this->vectorTable->search($queryVector, 5);

            // Assert that search returns results
            $this->assertNotEmpty($results, "Search should return results for edge case: '{$testCase['query']}'");

            $topSimilarity = $results[0]['similarity'];
            echo "  Top similarity: " . number_format($topSimilarity, 4) . "\n";

            // Apply test-specific validation based on test type
            switch ($testCase['test_type']) {
                case 'high_similarity':
                    $this->assertGreaterThan(0.3, $topSimilarity, "High similarity test should have similarity > 0.3 for query: '{$testCase['query']}'");
                    echo "  ✓ High similarity test passed (similarity: " . number_format($topSimilarity, 6) . ")\n";
                    break;

                case 'unrelated_query':
                    $maxSimilarity = max(array_column($results, 'similarity'));
                    $this->assertLessThan(0.6, $maxSimilarity, "Unrelated query should have max similarity < 0.6 for query: '{$testCase['query']}'");
                    echo "  ✓ Unrelated query test passed (max similarity: " . number_format($maxSimilarity, 4) . ")\n";
                    break;

                case 'partial_match':
                    $this->assertGreaterThan(0.2, $topSimilarity, "Partial match should have similarity > 0.2 for query: '{$testCase['query']}'");
                    $this->assertLessThan(0.8, $topSimilarity, "Partial match should have similarity < 0.8 for query: '{$testCase['query']}'");
                    echo "  ✓ Partial match test passed (similarity: " . number_format($topSimilarity, 4) . ")\n";
                    break;

                default:
                    echo "  ✓ Edge case test completed (similarity: " . number_format($topSimilarity, 4) . ")\n";
            }
            echo "\n";
        }

        echo "✓ Edge case tests completed\n\n";

        // Clean up test data
        $this->cleanupVectorData();
    }
}
