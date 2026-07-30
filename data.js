const { Redis } = require('@upstash/redis');

// Upstash 통합을 Vercel 프로젝트에 연결하면 아래 두 환경변수 중 하나의 조합이
// 자동으로 채워집니다: UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN,
// 또는 (구 Vercel KV 호환) KV_REST_API_URL / KV_REST_API_TOKEN.
// Redis.fromEnv()는 두 조합을 모두 자동으로 찾아 사용합니다.
const redis = Redis.fromEnv();

// 데이터를 저장할 키 (하나의 JSON 객체에 전체 데이터를 저장)
const DB_KEY = 'pt-mungyo-db';

// ---------------------------------------------------------------------------
// 동시 저장 시 lost update(덮어쓰기) 방지용 락
//
// Vercel Blob 버전과 같은 이유로 락이 필요합니다: 이 앱은 여러 기기/사람이
// 동시에 저장할 수 있는데, 락 없이 "읽기 → 합치기 → 쓰기"를 하면 두 저장이
// 겹칠 때 한쪽의 변경사항이 사라질 수 있습니다.
//
// Redis에서는 Blob보다 훨씬 간단하게 구현됩니다: SET ... NX EX는 Redis
// 서버에서 원자적(atomic)으로 처리되는 한 번의 명령이라, "이미 있으면 실패,
// 없으면 즉시 생성 + 자동 만료(TTL) 설정"이 별도의 재조회 로직 없이 됩니다.
// 락을 쥔 프로세스가 비정상 종료돼도 TTL이 지나면 자동으로 풀립니다.
// ---------------------------------------------------------------------------
const LOCK_KEY = 'pt-mungyo-db.lock';
const LOCK_TTL_SECONDS = 15; // 이 시간이 지난 락은 Redis가 자동으로 만료시킵니다.
const LOCK_WAIT_TOTAL_MS = 8000; // 이 시간 동안 락을 못 얻으면 503으로 응답 (클라이언트가 자동 재시도)
const LOCK_POLL_MS = 150;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// 주의: 이 함수는 절대로 에러를 조용히 삼켜서 null로 바꾸면 안 됩니다.
// (실제로는 인증/설정 오류인데 "아직 저장된 데이터가 없음"으로 잘못 판단하면,
//  그 뒤 저장 로직이 서버의 실제 데이터를 빈 값으로 덮어써 버리는 대형 사고로
//  이어집니다.) redis.get()이 정말 키가 없을 때만 null을 반환하고, 그 외의
// 문제(인증 오류, 네트워크 오류 등)는 예외를 던지므로 그대로 호출부로
// 전달해서 500 에러로 응답하고 클라이언트가 안전하게 로컬 캐시로 폴백하도록
// 합니다.
async function readCurrent() {
  const data = await redis.get(DB_KEY);
  return data || null;
}

// SET key value NX EX ttl : 키가 없을 때만 생성하고 TTL을 함께 건다 — 이
// 한 번의 명령이 원자적이라 Blob 버전처럼 "락 파일 존재 확인 → 강제 회수"
// 같은 별도 단계가 필요 없습니다.
async function acquireLock() {
  const start = Date.now();
  while (true) {
    const acquired = await redis.set(LOCK_KEY, String(Date.now()), { nx: true, ex: LOCK_TTL_SECONDS });
    if (acquired) return; // 락 획득 성공 ('OK' 또는 true가 반환되면 성공)
    if (Date.now() - start > LOCK_WAIT_TOTAL_MS) {
      const err = new Error('다른 저장 작업이 진행 중이라 시간 내에 락을 얻지 못했습니다. 잠시 후 다시 시도해주세요.');
      err.code = 'LOCK_TIMEOUT';
      throw err;
    }
    await sleep(LOCK_POLL_MS + Math.floor(Math.random() * 100));
  }
}

async function releaseLock() {
  try {
    await redis.del(LOCK_KEY);
  } catch (e) {
    // 삭제 실패해도 TTL이 있어 다음 요청이 언젠가는 회수할 수 있으므로 무시합니다.
    console.error('lock release failed (락 해제 실패, TTL로 자동 회수될 예정)', e);
  }
}

module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  try {
    if (req.method === 'GET') {
      const data = await readCurrent();
      res.status(200).json(data);
      return;
    }

    if (req.method === 'POST') {
      let body = req.body;
      if (typeof body === 'string') {
        try {
          body = JSON.parse(body || '{}');
        } catch (e) {
          res.status(400).json({ error: '잘못된 JSON 형식입니다.' });
          return;
        }
      }
      if (!body || typeof body !== 'object') body = {};

      // 부분 저장(patch) 지원: 클라이언트가 변경된 항목(키)만 보내면,
      // 서버에 저장된 기존 데이터와 병합해서 저장합니다.
      // 읽기→합치기→쓰기 전체를 락으로 감싸서, 동시에 들어온 다른 저장 요청이
      // 서로의 변경사항을 지워버리지 않도록 합니다 (자세한 설명은 위 주석 참고).
      await acquireLock();
      try {
        const current = await readCurrent();
        const merged = { ...(current || {}), ...body };
        await redis.set(DB_KEY, merged);
      } finally {
        await releaseLock();
      }
      res.status(200).json({ ok: true });
      return;
    }

    res.status(405).json({ error: 'Method Not Allowed' });
  } catch (err) {
    console.error('data function error', err);
    const status = err && err.code === 'LOCK_TIMEOUT' ? 503 : 500;
    res.status(status).json({
      error: err && err.code === 'LOCK_TIMEOUT' ? err.message : '서버 오류가 발생했습니다.',
      debug_name: err && err.name,
      debug_message: err && err.message,
    });
  }
};
