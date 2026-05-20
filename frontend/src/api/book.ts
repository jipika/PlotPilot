/**
 * 书目兼容 API — 已迁移方法委托 v1 客户端；仅保留尚无 v1 对等端的 legacy 路径。
 */
import type {
  BookListItem,
  BookDeskResponse,
  CastGraph,
  CastSearchResponse,
  CastCoverage,
  StoryKnowledge,
  KnowledgeSearchResponse,
  Bible,
  ChapterBody,
  ChapterReview,
  ChapterReviewAiResponse,
  ChapterStructure,
  SimpleResponse,
  SlugResponse,
  JobCreateResponse,
  JobStatusResponse,
} from '../types/api'
import { legacyBookHttp } from './config'
import { castApi } from './cast'
import { knowledgeApi } from './knowledge'
import { chapterApi } from './chapter'
import { bibleApi } from './bible'

const legacy = legacyBookHttp

/** slug 与 novel_id 在本产品中通常为同一标识 */
const asNovelId = (slug: string) => slug

export const bookApi = {
  getList: () => legacy.get<BookListItem[]>('/books') as unknown as Promise<BookListItem[]>,
  create: (data: unknown) =>
    legacy.post<SlugResponse>('/jobs/create-book', data) as unknown as Promise<SlugResponse>,
  deleteBook: (slug: string) =>
    legacy.delete<SimpleResponse>(`/book/${slug}`) as unknown as Promise<SimpleResponse>,

  getCast: (slug: string) => castApi.getCast(asNovelId(slug)) as unknown as Promise<CastGraph>,
  putCast: (slug: string, data: unknown) => castApi.putCast(asNovelId(slug), data as CastGraph),
  searchCast: (slug: string, q: string) =>
    castApi.searchCast(asNovelId(slug), q) as unknown as Promise<CastSearchResponse>,
  getCastCoverage: (slug: string) =>
    castApi.getCastCoverage(asNovelId(slug)) as unknown as Promise<CastCoverage>,

  getKnowledge: (slug: string) =>
    knowledgeApi.getKnowledge(asNovelId(slug)) as unknown as Promise<StoryKnowledge>,
  putKnowledge: (slug: string, data: unknown) =>
    knowledgeApi.putKnowledge(asNovelId(slug), data as StoryKnowledge),
  knowledgeSearch: (slug: string, q: string, k = 6) =>
    knowledgeApi.searchKnowledge(asNovelId(slug), q, k) as unknown as Promise<KnowledgeSearchResponse>,

  getDesk: (slug: string) =>
    legacy.get<BookDeskResponse>(`/book/${slug}/desk`) as unknown as Promise<BookDeskResponse>,

  getBible: (slug: string) => bibleApi.getBible(asNovelId(slug)) as unknown as Promise<Bible>,
  saveBible: (slug: string, data: unknown) => bibleApi.updateBible(asNovelId(slug), data),

  getChapterBody: async (slug: string, chapterId: number) => {
    const ch = await chapterApi.getChapter(asNovelId(slug), chapterId)
    return { content: ch.content } as ChapterBody
  },
  saveChapterBody: (slug: string, chapterId: number, content: string) =>
    chapterApi.updateChapter(asNovelId(slug), chapterId, { content }),

  getChapterReview: async (slug: string, chapterId: number) => {
    const r = await chapterApi.getChapterReview(asNovelId(slug), chapterId)
    return r as unknown as ChapterReview
  },
  saveChapterReview: (slug: string, chapterId: number, status: string, memo: string) =>
    chapterApi.saveChapterReview(asNovelId(slug), chapterId, status, memo),
  reviewChapterAi: (slug: string, chapterId: number, save = false) =>
    chapterApi.reviewChapterAi(asNovelId(slug), chapterId, save) as unknown as Promise<ChapterReviewAiResponse>,
  getChapterStructure: async (slug: string, chapterId: number) => {
    const s = await chapterApi.getChapterStructure(asNovelId(slug), chapterId)
    return s as unknown as ChapterStructure
  },
}

export const jobApi = {
  startPlan: (slug: string, dryRun = false, mode: 'initial' | 'revise' = 'initial') =>
    legacy.post<JobCreateResponse>(`/jobs/${slug}/plan`, { dry_run: dryRun, mode }) as unknown as Promise<JobCreateResponse>,
  startWrite: (slug: string, from: number, to?: number, dryRun = false, continuity = false) =>
    legacy.post<JobCreateResponse>(`/jobs/${slug}/write`, {
      from_chapter: from,
      to_chapter: to,
      dry_run: dryRun,
      continuity,
    }) as unknown as Promise<JobCreateResponse>,
  startRun: (slug: string, dryRun = false, continuity = false) =>
    legacy.post<JobCreateResponse>(`/jobs/${slug}/run`, { dry_run: dryRun, continuity }) as unknown as Promise<JobCreateResponse>,
  getJobStatus: (slug: string, jobId: string) =>
    legacy.get<JobStatusResponse>(`/jobs/${slug}/${jobId}`) as unknown as Promise<JobStatusResponse>,
}
