import pool from "../db/db";

// Service class for handling search queries
export class QueryService {
    /**
     * Searches for pages matching the given keywords.
     * @param keywords Array of keywords to search for.
     * @param page Page number for pagination (default: 1).
     * @param limit Number of results per page (default: 10).
     * @returns An object containing the search results and total count.
     */
    async searchByKeywords(
        keywords: string[],
        page = 1,
        limit = 10
    ): Promise<{ results: any[]; total: number }> {
        // Return empty result if no keywords provided
        if (!keywords.length) return { results: [], total: 0 };

        // Prepare SQL placeholders for keyword parameters
        const placeholders = keywords.map(() => '?').join(', ');
        const offset = (page - 1) * limit;

        const totalInputKeywords = keywords.length;

        // Query to fetch matching pages with relevance score
        const [results] = await pool.query(
            `
            SELECT 
                cq.url,
                SUM(ii.frequency) AS total_frequency,
                COUNT(DISTINCT ii.keyword) AS matched_keywords,
                (SUM(ii.frequency) * COUNT(DISTINCT ii.keyword) / ?) AS relevance_score
            FROM inverted_index ii
            JOIN crawler_queue cq ON ii.page_id = cq.id
            WHERE ii.keyword IN (${placeholders})
            GROUP BY ii.page_id
            ORDER BY relevance_score DESC
            LIMIT ? OFFSET ?
            `,
            [totalInputKeywords, ...keywords, limit, offset]
        );

        // Query to count total number of matching pages
        const [countRows] = await pool.query(
            `
            SELECT COUNT(DISTINCT ii.page_id) AS total
            FROM inverted_index ii
            JOIN crawler_queue cq ON ii.page_id = cq.id
            WHERE ii.keyword IN (${placeholders})
            `,
            keywords
        );

        // Extract total count from query result
        const total = (countRows as any[])[0]?.total || 0;

        // Return results and total count
        return {
            results: results as any[],
            total,
        };
    }
}