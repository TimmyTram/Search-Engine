import { Request, Response } from 'express';


export const health = async (req: Request, res: Response): Promise<void> => {
    res.status(200).json({
        message: 'Query Engine is Live',
    });
}