import { Request, Response } from 'express';
import { QueryService } from '../services/queryService';

const queryService = new QueryService();

export const search = async (req: Request, res: Response): Promise<void> => {
    try {
        const rawKeywords = req.query.q as string;
        const page = parseInt(req.query.page as string) || 1;
        const limit = parseInt(req.query.limit as string) || 10;

        if (!rawKeywords) {
            res.status(400).json({ error: 'Missing "q" query parameter' });
            return;
        }

        const keywordList = rawKeywords.split(',').map(k => k.trim()).filter(Boolean);
        const { results, total } = await queryService.searchByKeywords(keywordList, page, limit);

        res.status(200).json({
            page,
            limit,
            total,
            totalPages: Math.ceil(total / limit),
            keywords: keywordList,
            results,
        });
    } catch (error) {
        console.error('Search failed:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
};

