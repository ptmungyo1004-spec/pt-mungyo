const { put, get, del } = require('@vercel/blob');

// 데이터를 저장할 파일 이름 (하나의 JSON 파일에 전체 데이터를 저장)
const BLOB_PATHNAME = 'pt-mungyo-db.json';

// 주의: 이 함수는 절대로 에러를 조용히 삼켜서 null로 바꾸면 안 됩니다.
// (실제로는 인증/설정 오류인데 "아직 저장된 데이터가 없음"으로 잘못 판단하면,
//  그 뒤 저장 로직이 서버의 실제 데이터를 빈 값으로 덮어써 버리는 대형 사고로
//  이어집니다. 실제로 한 번 이 문제로 데이터가 초기화된 적이 있습니다.)
// get()은 blob이 정말 존재하지 않을 때만 null을 반환하고, 그 외의 문제는
// 예외(exception)를 던집니다 — 그 예외는 여기서 잡지 않고 그대로 호출부로
// 전달해서, 호출부가 500 에러로 응답하고 클라이언트가 안전하게 로컬 캐시로
// 폴백하도록 합니다.
// Vercel 프로젝트가 OIDC로 이 store에 연결되어 있으면 SDK가 기본적으로 OIDC 토큰을
// 사용하는데, 환경에 따라 OIDC 토큰의 access scope가 제대로 반영되지 않아
// "Cannot use public access on a private store" 오류가 나는 경우가 있습니다.
// BLOB_READ_WRITE_TOKEN이 환경변수로 존재하면 그걸 명시적으로 사용해서
// OIDC 경로를 우회하고 이 문제를 피합니다.
const blobAuthOptions = process.env.BLOB_READ_WRITE_TOKEN
  ? { token: process.env.BLOB_READ_WRITE_TOKEN }
  : {};

// ---------------------------------------------------------------------------
// 버그 수정: 동시 저장 시 lost update(덮어쓰기) 방지
//
// 기존 코드는 "현재 데이터 읽기 → 합치기 → 통째로 쓰기"를 아무 잠금 없이
// 실행했습니다. 이 앱은 여러 기기/사람이 동시에 접속해 저장할 수 있는데,
// 두 개의 저장 요청이 거의 동시에 들어오면 다음과 같은 문제가 생깁니다.
//
//   요청 A: 현재 데이터(S0)를 읽음
//   요청 B: (A가 아직 쓰기 전) 역시 S0를 읽음
//   요청 A: S0 + A의 변경사항을 저장  ← 이 시점엔 정상적으로 저장된 것처럼 보임
//   요청 B: S0(= A의 변경사항이 반영되기 전 스냅샷) + B의 변경사항을 저장
//           → A가 방금 저장한 내용이 통째로 사라짐 (lost update)
//
// 실제 증상: 생산입고를 저장하면 잠깐 반영됐다가, 몇 초 뒤(자동 동기화 시)
// 감쪽같이 사라지고 주문서 상태도 되돌아가는 것처럼 보였던 원인이 이것입니다.
//
// 해결: Vercel Blob의 put()은 allowOverwrite:false(기본값)일 때 이미 존재하는
// 파일에 쓰려고 하면 에러를 던집니다. 이 성질을 이용해 "락 파일이 없을 때만
// 생성 가능"한 원자적(atomic) 락을 만들고, 저장(읽기→합치기→쓰기) 작업 전체를
// 이 락으로 감쌉니다. 락을 쥔 프로세스가 비정상 종료돼도 무한정 잠기지 않도록
// TTL(자동 만료)을 둡니다.
// ---------------------------------------------------------------------------
const LOCK_PATHNAME = 'pt-mungyo-db.lock.json';
const LOCK_TTL_MS = 15000; // 이 시간이 지난 락은 죽은 락으로 간주하고 강제로 회수합니다.
const LOCK_WAIT_TOTAL_MS = 8000; // 이 시간 동안 락을 못 얻으면 503으로 응답 (클라이언트가 자동 재시도)
const LOCK_POLL_MS = 150;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function readJsonBlob(pathname) {
  // 중요(진짜 원인이었던 버그): Vercel Blob은 private blob이라도 get()의 기본
  // 동작이 CDN 캐시를 거치기 때문에, 방금 put()으로 덮어쓴 직후에 바로 읽으면
  // 캐시 전파 지연(최대 60초)으로 예전 버전이 반환될 수 있습니다. 이게 바로
  // "저장은 성공했는데 몇 초/몇십 초 뒤 자동 동기화 때 사라지는" 현상의
  // 근본 원인이었습니다. useCache:false를 주면 캐시를 건너뛰고 원본에서
  // 항상 최신 데이터를 읽습니다.
  const result = await get(pathname, { access: 'private', useCache: false, ...blobAuthOptions });
  if (!result || !result.stream) return null; // 정말로 존재하지 않는 경우만 null
  const chunks = [];
  const reader = result.stream.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  const text = Buffer.concat(chunks.map((c) => Buffer.from(c))).toString('utf-8');
  if (!text) return null;
  return JSON.parse(text);
}

async function readCurrent() {
  return readJsonBlob(BLOB_PATHNAME);
}

async function tryCreateLock() {
  // allowOverwrite:false(기본값) → 이미 파일이 있으면 예외를 던짐.
  // 이 "존재하지 않을 때만 성공" 특성을 compare-and-swap 대용으로 사용합니다.
  await put(
    LOCK_PATHNAME,
    JSON.stringify({ acquiredAt: Date.now(), expiresAt: Date.now() + LOCK_TTL_MS }),
    {
      access: 'private',
      contentType: 'application/json; charset=utf-8',
      addRandomSuffix: false,
      allowOverwrite: false,
      ...blobAuthOptions,
    }
  );
}

async function forceOverwriteLock() {
  await put(
    LOCK_PATHNAME,
    JSON.stringify({ acquiredAt: Date.now(), expiresAt: Date.now() + LOCK_TTL_MS }),
    {
      access: 'private',
      contentType: 'application/json; charset=utf-8',
      addRandomSuffix: false,
      allowOverwrite: true,
      ...blobAuthOptions,
    }
  );
}

// 저장 작업(읽기→합치기→쓰기) 전체를 감싸는 락을 획득합니다.
// 이미 다른 요청이 락을 쥐고 있으면, 그 락이 살아있는 동안은 잠깐씩 대기하며
// 재시도하고, 락이 만료(TTL 초과)됐으면 죽은 락으로 보고 강제로 회수합니다.
async function acquireLock() {
  const start = Date.now();
  while (true) {
    try {
      await tryCreateLock();
      return; // 락 획득 성공
    } catch (e) {
      let existing = null;
      try {
        existing = await readJsonBlob(LOCK_PATHNAME);
      } catch (_) {
        // 락 파일을 못 읽어도(권한/네트워크 순간 오류 등) 아래에서 대기 후 재시도합니다.
      }
      const isStale = !existing || !existing.expiresAt || existing.expiresAt < Date.now();
      if (isStale) {
        try {
          await forceOverwriteLock();
          return; // 죽은 락을 회수해서 획득 성공
        } catch (_) {
          // 회수 시도 중 다른 요청과 또 충돌했을 수 있음 - 아래 대기 로직으로 넘어감
        }
      }
      if (Date.now() - start > LOCK_WAIT_TOTAL_MS) {
        const err = new Error('다른 저장 작업이 진행 중이라 시간 내에 락을 얻지 못했습니다. 잠시 후 다시 시도해주세요.');
        err.code = 'LOCK_TIMEOUT';
        throw err;
      }
      await sleep(LOCK_POLL_MS + Math.floor(Math.random() * 100));
    }
  }
}

async function releaseLock() {
  try {
    await del(LOCK_PATHNAME, { ...blobAuthOptions });
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
      // readCurrent()가 실패(예외)하면 여기서 잡지 않고 아래 catch로 넘어가
      // 500을 응답합니다 — 즉, 기존 데이터를 확실히 읽지 못한 상태에서는
      // 절대로 덮어쓰기(put)를 실행하지 않습니다.
      //
      // 읽기→합치기→쓰기 전체를 락으로 감싸서, 동시에 들어온 다른 저장 요청이
      // 서로의 변경사항을 지워버리지 않도록 합니다 (자세한 설명은 위 주석 참고).
      await acquireLock();
      try {
        const current = await readCurrent();
        const merged = { ...(current || {}), ...body };

        await put(BLOB_PATHNAME, JSON.stringify(merged), {
          access: 'private',
          contentType: 'application/json; charset=utf-8',
          addRandomSuffix: false,
          allowOverwrite: true,
          ...blobAuthOptions,
        });
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
