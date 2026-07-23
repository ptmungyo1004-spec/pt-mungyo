const { put, get } = require('@vercel/blob');

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

async function readCurrent() {
  const result = await get(BLOB_PATHNAME, { access: 'private', ...blobAuthOptions });
  if (!result || !result.stream) return null; // 최초 실행 등, 정말로 데이터가 없는 경우만 null
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
      const current = await readCurrent();
      const merged = { ...(current || {}), ...body };

      await put(BLOB_PATHNAME, JSON.stringify(merged), {
        access: 'private',
        contentType: 'application/json; charset=utf-8',
        addRandomSuffix: false,
        allowOverwrite: true,
        ...blobAuthOptions,
      });
      res.status(200).json({ ok: true });
      return;
    }

    res.status(405).json({ error: 'Method Not Allowed' });
  } catch (err) {
    console.error('data function error', err);
    res.status(500).json({
      error: '서버 오류가 발생했습니다.',
      debug_name: err && err.name,
      debug_message: err && err.message,
    });
  }
};
