#!/usr/bin/env node
// Usage:
//   node cluster.js in.bpmn out.bpmn --mode=fragment --threshold=0.7 [--no-singletons] [--clear-old]
//   node cluster.js in.bpmn out.bpmn --mode=mask --privacy=0.5 [--clear-old]

const fs = require('fs');
const { DOMParser, XMLSerializer } = require('@xmldom/xmldom');
const xpath = require('xpath');

const NS = {
  bpmn: 'http://www.omg.org/spec/BPMN/20100524/MODEL',
  bpmndi: 'http://www.omg.org/spec/BPMN/20100524/DI',
  dc: 'http://www.omg.org/spec/DD/20100524/DC',
  di: 'http://www.omg.org/spec/DD/20100524/DI',
  cpl: 'http://example.com/schema/coupling',
};
const select = xpath.useNamespaces(NS);

function parseArgs() {
  const args = process.argv.slice(2);
  if (args.length < 2) {
    console.error(
      'Usage: node cluster.js input.bpmn output.bpmn [--mode=fragment|mask] [--threshold=0.7] [--privacy=0.5] [--no-singletons] [--clear-old]'
    );
    process.exit(1);
  }
  const opts = {
    input: args[0],
    output: args[1],
    mode: 'fragment',
    threshold: 0.7,
    privacy: 0.5,
    privacyDir: 'below',
    includeSingletons: true,
    clearOld: false,
  };
  for (const a of args.slice(2)) {
    let m;
    if ((m = a.match(/^--mode=(.+)$/))) opts.mode = m[1];
    else if ((m = a.match(/^--threshold=(.+)$/))) opts.threshold = parseFloat(m[1]);
    else if ((m = a.match(/^--privacy=(.+)$/))) opts.privacy = parseFloat(m[1]);
    else if ((m = a.match(/^--privacy-dir=(above|below)$/))) opts.privacyDir = m[1];
    else if (a === '--no-singletons') opts.includeSingletons = false;
    else if (a === '--clear-old') opts.clearOld = true;
  }
  return opts;
}

function unionFind(n) {
  const parent = Array.from({ length: n }, (_, i) => i);
  const find = (x) => (parent[x] === x ? x : (parent[x] = find(parent[x])));
  const unite = (a, b) => {
    a = find(a);
    b = find(b);
    if (a !== b) parent[b] = a;
  };
  const groups = () => {
    const g = new Map();
    parent.forEach((_, i) => {
      const r = find(i);
      if (!g.has(r)) g.set(r, []);
      g.get(r).push(i);
    });
    return [...g.values()];
  };
  return { find, unite, groups };
}

function ensurePlane(defs, processEl, doc) {
  let plane = select('//bpmndi:BPMNDiagram/bpmndi:BPMNPlane', doc)[0];
  if (!plane) {
    const diagram = doc.createElementNS(NS.bpmndi, 'bpmndi:BPMNDiagram');
    diagram.setAttribute('id', 'BPMNDiagram_Auto');
    plane = doc.createElementNS(NS.bpmndi, 'bpmndi:BPMNPlane');
    plane.setAttribute('id', 'BPMNPlane_Auto');
    plane.setAttribute('bpmnElement', processEl.getAttribute('id'));
    diagram.appendChild(plane);
    defs.appendChild(diagram);
  }
  return plane;
}

function boundsMap(plane) {
  const m = new Map();
  const shapes = select('.//bpmndi:BPMNShape', plane);
  shapes.forEach((s) => {
    const elId = s.getAttribute('bpmnElement');
    const b = select('./dc:Bounds', s)[0];
    if (elId && b)
      m.set(elId, {
        x: parseFloat(b.getAttribute('x')),
        y: parseFloat(b.getAttribute('y')),
        w: parseFloat(b.getAttribute('width')),
        h: parseFloat(b.getAttribute('height')),
      });
  });
  return m;
}

function rectFromBoundsEl(b) {
  const x = parseFloat(b.getAttribute('x'));
  const y = parseFloat(b.getAttribute('y'));
  const w = parseFloat(b.getAttribute('width'));
  const h = parseFloat(b.getAttribute('height'));
  return { x, y, w, h, cx: x + w/2, cy: y + h/2 };
}

function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }

/**
 * Return an anchor point on the rectangle EDGE that faces toward (tx, ty).
 * We choose the side by comparing dx vs dy, then clamp along that side.
 */
function edgeAnchor(boundsEl, tx, ty) {
  const r = rectFromBoundsEl(boundsEl);
  const dx = tx - r.cx;
  const dy = ty - r.cy;

  if (Math.abs(dx) >= Math.abs(dy)) {
    // horizontal approach: left/right side
    if (dx >= 0) {
      // right edge
      return { x: r.x + r.w, y: clamp(ty, r.y, r.y + r.h) };
    } else {
      // left edge
      return { x: r.x, y: clamp(ty, r.y, r.y + r.h) };
    }
  } else {
    // vertical approach: top/bottom side
    if (dy >= 0) {
      // bottom edge
      return { x: clamp(tx, r.x, r.x + r.w), y: r.y + r.h };
    } else {
      // top edge
      return { x: clamp(tx, r.x, r.x + r.w), y: r.y };
    }
  }
}


function addTextAnnotationForGroup(doc, defs, plane, processEl, groupId, label, x, y) {
  // Create the TextAnnotation
  const taId = `${groupId}_TA`;
  const ta = doc.createElementNS(NS.bpmn, 'bpmn:textAnnotation');
  ta.setAttribute('id', taId);
  const text = doc.createElementNS(NS.bpmn, 'bpmn:text');
  text.appendChild(doc.createTextNode(label));
  ta.appendChild(text);
  processEl.appendChild(ta);

  // Association from annotation -> group (direction None)
  const assoc = doc.createElementNS(NS.bpmn, 'bpmn:association');
  assoc.setAttribute('id', `${groupId}_TA_Assoc`);
  assoc.setAttribute('associationDirection', 'None');
  assoc.setAttribute('sourceRef', taId);
  assoc.setAttribute('targetRef', groupId);
  processEl.appendChild(assoc);

  // DI for the TextAnnotation
  const taShape = doc.createElementNS(NS.bpmndi, 'bpmndi:BPMNShape');
  taShape.setAttribute('id', `${taId}_di`);
  taShape.setAttribute('bpmnElement', taId);
  const taBounds = doc.createElementNS(NS.dc, 'dc:Bounds');
  taBounds.setAttribute('x', String(x));
  taBounds.setAttribute('y', String(y));
  taBounds.setAttribute('width', '160');
  taBounds.setAttribute('height', '40');
  taShape.appendChild(taBounds);
  plane.appendChild(taShape);

  // DI edge for the association: edge-to-edge (no center)
  const gBounds = xpath.useNamespaces(NS)(
    `.//bpmndi:BPMNShape[@bpmnElement="${groupId}"]/dc:Bounds`,
    plane
  )[0];

  if (gBounds) {
    const rA = rectFromBoundsEl(taBounds);
    const rG = rectFromBoundsEl(gBounds);

    // choose anchor points on the EDGES facing each other
    const groupAnchor = edgeAnchor(gBounds, rA.cx, rA.cy);
    const annoAnchor  = edgeAnchor(taBounds, rG.cx, rG.cy);

    // draw a straight line from group edge to annotation edge
    const e = doc.createElementNS(NS.bpmndi, 'bpmndi:BPMNEdge');
    e.setAttribute('id', `${groupId}_TA_Assoc_di`);
    e.setAttribute('bpmnElement', `${groupId}_TA_Assoc`);

    const w1 = doc.createElementNS(NS.di, 'di:waypoint');
    w1.setAttribute('x', String(groupAnchor.x));
    w1.setAttribute('y', String(groupAnchor.y));
    const w2 = doc.createElementNS(NS.di, 'di:waypoint');
    w2.setAttribute('x', String(annoAnchor.x));
    w2.setAttribute('y', String(annoAnchor.y));

    e.appendChild(w1);
    e.appendChild(w2);
    plane.appendChild(e);
  }
}


function getTaskList(processEl) {
  return select(
    './*[self::bpmn:task or self::bpmn:userTask or self::bpmn:serviceTask or self::bpmn:scriptTask or self::bpmn:manualTask or self::bpmn:businessRuleTask or self::bpmn:sendTask or self::bpmn:receiveTask]',
    processEl
  );
}

function clearOldFragments(doc, processEl, defs) {
  // Remove old groups with cpl:fragmentId or id starting with Fragment_
  const oldGroups = select('.//bpmn:group', processEl).filter(
    (g) =>
      g.getAttributeNS(NS.cpl, 'fragmentId') != null ||
      /^Fragment_/.test(g.getAttribute('id') || '')
  );
  oldGroups.forEach((g) => g.parentNode.removeChild(g));

  // Remove their DI shapes
  const plane = ensurePlane(defs, processEl, doc);
  select('.//bpmndi:BPMNShape', plane).forEach((s) => {
    const be = s.getAttribute('bpmnElement') || '';
    if (/^Fragment_/.test(be)) s.parentNode.removeChild(s);
  });

  // Remove their associations
  select('.//bpmn:association', processEl).forEach((a) => {
    const src = a.getAttribute('sourceRef') || '';
    if (/^Fragment_/.test(src)) a.parentNode.removeChild(a);
  });
}

// --- FRAGMENT (now includes singletons by default) ---
function fragmentByCoupling(doc, defs, processEl, threshold, includeSingletons) {
  const tasks = getTaskList(processEl);
  const idToIdx = new Map(tasks.map((t, i) => [t.getAttribute('id'), i]));
  const uf = unionFind(tasks.length);

  const flows = select('.//bpmn:sequenceFlow', processEl);
  flows.forEach((f) => {
    const weightStr = f.getAttribute('cpl:coupling') || f.getAttributeNS(NS.cpl, 'coupling');
    if (!weightStr) return;
    const w = parseFloat(weightStr);
    if (!(w >= threshold)) return;
    const src = f.getAttribute('sourceRef');
       const tgt = f.getAttribute('targetRef');
    if (idToIdx.has(src) && idToIdx.has(tgt)) uf.unite(idToIdx.get(src), idToIdx.get(tgt));
  });

  let comps = uf.groups();
  if (!includeSingletons) comps = comps.filter((g) => g.length > 1);

  // DI for groups
  const plane = ensurePlane(defs, processEl, doc);
  const bmap = boundsMap(plane);

  // Category
  let cat = select('./bpmn:category[@id="Category_Fragments"]', defs)[0];
  if (!cat) {
    cat = doc.createElementNS(NS.bpmn, 'bpmn:category');
    cat.setAttribute('id', 'Category_Fragments');
    defs.insertBefore(cat, defs.firstChild);
  }

  comps.forEach((indices, idx) => {
    const fragId = 'Fragment_' + (idx + 1);

    const cv = doc.createElementNS(NS.bpmn, 'bpmn:categoryValue');
    cv.setAttribute('id', fragId + '_CV');
    cv.setAttribute('value', fragId); // <-- semantic name (BPMN-native)
    cat.appendChild(cv);

    const group = doc.createElementNS(NS.bpmn, 'bpmn:group');
    group.setAttribute('id', fragId);
    group.setAttribute('categoryValueRef', fragId + '_CV');
    group.setAttributeNS(NS.cpl, 'cpl:fragmentId', fragId);
    processEl.appendChild(group);

    const memberIds = indices.map((i) => tasks[i].getAttribute('id'));

    // bbox from member tasks (works for singletons too)
    let minx = Infinity,
      miny = Infinity,
      maxx = -Infinity,
      maxy = -Infinity;
    memberIds.forEach((id) => {
      const b = bmap.get(id) || { x: 100, y: 100, w: 100, h: 80 };
      minx = Math.min(minx, b.x);
      miny = Math.min(miny, b.y);
      maxx = Math.max(maxx, b.x + b.w);
      maxy = Math.max(maxy, b.y + b.h);
    });
    const pad = 24;
    minx -= pad;
    miny -= pad;
    maxx += pad;
    maxy += pad;

    const gShape = doc.createElementNS(NS.bpmndi, 'bpmndi:BPMNShape');
    gShape.setAttribute('id', fragId + '_di');
    gShape.setAttribute('bpmnElement', fragId);
    const bounds = doc.createElementNS(NS.dc, 'dc:Bounds');
    bounds.setAttribute('x', String(minx));
    bounds.setAttribute('y', String(miny));
    bounds.setAttribute('width', String(maxx - minx));
    bounds.setAttribute('height', String(maxy - miny));
    gShape.appendChild(bounds);
    plane.appendChild(gShape);

    // optional associations
    memberIds.forEach((mid) => {
      const assoc = doc.createElementNS(NS.bpmn, 'bpmn:association');
      assoc.setAttribute('id', `${fragId}_A_${mid}`);
      assoc.setAttribute('associationDirection', 'None');
      assoc.setAttribute('sourceRef', fragId);
      assoc.setAttribute('targetRef', mid);
      processEl.appendChild(assoc);
    });

    // Add machine-readable annotations on the group
    group.setAttributeNS(NS.cpl, 'cpl:fragmentName', fragId);
    group.setAttributeNS(NS.cpl, 'cpl:fragmentSize', String(memberIds.length));
    group.setAttributeNS(NS.cpl, 'cpl:couplingThreshold', String(threshold));

    // Build a human-readable label for the TextAnnotation
    // (you can enrich with stats: e.g., internal edges count/avg coupling)
    const label = `${fragId}\nsize=${memberIds.length}`;

    // Place the note just above the fragment box
    const noteX = parseFloat(bounds.getAttribute('x'));
    const noteY = parseFloat(bounds.getAttribute('y')) - 48; // 48 px above
    addTextAnnotationForGroup(doc, defs, plane, processEl, fragId, label, noteX, noteY);
  });

  return comps.length;
}

// Remove a BPMNShape for a given element (task, annotation, group…)
function removeDIShapeForElement(plane, elId) {
  select(`.//bpmndi:BPMNShape[@bpmnElement="${elId}"]`, plane).forEach((n) => {
    if (n && n.parentNode) n.parentNode.removeChild(n);
  });
}

// Remove a BPMNEdge for a given edge-like element (association, sequenceFlow…)
function removeDIEdgeForElement(plane, elId) {
  select(`.//bpmndi:BPMNEdge[@bpmnElement="${elId}"]`, plane).forEach((n) => {
    if (n && n.parentNode) n.parentNode.removeChild(n);
  });
}

// Count how many associations still reference this element (as source or target)
function countAssociationsTouching(processEl, elId) {
  const touching = select(
    `.//bpmn:association[@sourceRef="${elId}" or @targetRef="${elId}"]`,
    processEl
  );
  return touching.length;
}

// If a textAnnotation has no remaining associations, delete it (and its DI)
function maybeRemoveTextAnnotationIfOrphaned(doc, defs, plane, processEl, taId) {
  // still referenced?
  if (countAssociationsTouching(processEl, taId) > 0) return;

  // remove the annotation element itself
  const ta = select(`.//bpmn:textAnnotation[@id="${taId}"]`, processEl)[0];
  if (ta && ta.parentNode) ta.parentNode.removeChild(ta);

  // remove its DI shape
  removeDIShapeForElement(plane, taId);
}

// Remove a specific association, its DI, and clean up any now-orphaned textAnnotations it connected
function removeAssociationCascade(doc, defs, plane, processEl, assocNode) {
  if (!assocNode) return;
  const assocId = assocNode.getAttribute('id');

  const src = assocNode.getAttribute('sourceRef');
  const tgt = assocNode.getAttribute('targetRef');

  // remove DI for the association edge
  removeDIEdgeForElement(plane, assocId);

  // remove the association element
  if (assocNode.parentNode) assocNode.parentNode.removeChild(assocNode);

  // if either endpoint is a textAnnotation, and now orphaned, remove it (+DI)
  const srcIsTA = !!select(`.//bpmn:textAnnotation[@id="${src}"]`, processEl)[0];
  const tgtIsTA = !!select(`.//bpmn:textAnnotation[@id="${tgt}"]`, processEl)[0];

  if (srcIsTA) maybeRemoveTextAnnotationIfOrphaned(doc, defs, plane, processEl, src);
  if (tgtIsTA) maybeRemoveTextAnnotationIfOrphaned(doc, defs, plane, processEl, tgt);
}

// Remove all associations touching a given element id (task id, flow id, group id…)
function removeAllAssociationsTouchingId(doc, defs, plane, processEl, elId) {
  // We must collect into an array first (live NodeList safety)
  const touching = select(
    `.//bpmn:association[@sourceRef="${elId}" or @targetRef="${elId}"]`,
    processEl
  ).slice();

  touching.forEach((a) => removeAssociationCascade(doc, defs, plane, processEl, a));
}

function existingFlow(processEl, src, tgt) {
  return select(`./bpmn:sequenceFlow[@sourceRef="${src}" and @targetRef="${tgt}"]`, processEl)[0];
}

function collectBoundaryNeighbors(mid, flowsBySource, flowsByTarget, maskedSet) {
  // BFS to unmasked predecessors (via masked-only)
  const preds = new Set();
  const qUp = [mid];
  const seenUp = new Set([mid]);
  while (qUp.length) {
    const cur = qUp.shift();
    const incoming = flowsByTarget.get(cur) || [];
    incoming.forEach((f) => {
      const s = f.getAttribute('sourceRef');
      if (maskedSet.has(s)) {
        if (!seenUp.has(s)) { seenUp.add(s); qUp.push(s); }
      } else {
        preds.add(s);
      }
    });
  }

  // BFS to unmasked successors (via masked-only)
  const succs = new Set();
  const qDown = [mid];
  const seenDown = new Set([mid]);
  while (qDown.length) {
    const cur = qDown.shift();
    const outgoing = flowsBySource.get(cur) || [];
    outgoing.forEach((f) => {
      const t = f.getAttribute('targetRef');
      if (maskedSet.has(t)) {
        if (!seenDown.has(t)) { seenDown.add(t); qDown.push(t); }
      } else {
        succs.add(t);
      }
    });
  }

  return { preds: [...preds], succs: [...succs] };
}

// --- MASK (unchanged from v3) ---
function maskByPrivacy(doc, defs, processEl, privacyThreshold, privacyDir = 'above') {
  const plane = ensurePlane(defs, processEl, doc);
  const tasks = getTaskList(processEl);
  const byId = new Map(tasks.map((t) => [t.getAttribute('id'), t]));

    // Decide which tasks to mask based on direction
  const shouldMask = (p) =>
    Number.isFinite(p)
      ? (privacyDir === 'above' ? p >= privacyThreshold : p < privacyThreshold)
      : false; // skip tasks without a numeric privacy value

  const maskedIds = [];

  tasks.forEach((t) => {
    const pStr = t.getAttribute('cpl:privacy') || t.getAttributeNS(NS.cpl, 'privacy');
    const p = pStr == null ? NaN : parseFloat(pStr);
    if (shouldMask(p)) maskedIds.push(t.getAttribute('id'));
  });
  if (maskedIds.length === 0) return 0;

  const allFlows = select('./bpmn:sequenceFlow', processEl);
  const flowsBySource = new Map();
  const flowsByTarget = new Map();
  allFlows.forEach((f) => {
    const s = f.getAttribute('sourceRef');
    const t = f.getAttribute('targetRef');
    if (!flowsBySource.has(s)) flowsBySource.set(s, []);
    if (!flowsByTarget.has(t)) flowsByTarget.set(t, []);
    flowsBySource.get(s).push(f);
    flowsByTarget.get(t).push(f);
  });

  let counter = 0;
  const removedFlowIds = new Set();
  const autoFlowPairs = new Set(); // dedupe src->tgt

  maskedIds.forEach((mid) => {
    const incoming = (flowsByTarget.get(mid) || []).slice();
    const outgoing = (flowsBySource.get(mid) || []).slice();

    const maskedSet = new Set(maskedIds);

    // ---- MULTI-HOP BYPASS BEFORE REMOVALS ----
    maskedIds.forEach((mid) => {
      const { preds, succs } = collectBoundaryNeighbors(mid, flowsBySource, flowsByTarget, maskedSet);

      preds.forEach((src) => {
        succs.forEach((tgt) => {
          if (src === tgt) return;                              // avoid self-loops
          const pairKey = `${src}→${tgt}`;
          if (autoFlowPairs.has(pairKey)) return;               // dedupe within this run
          if (existingFlow(processEl, src, tgt)) return;        // don't duplicate existing model edges
          autoFlowPairs.add(pairKey);

          const newId = `AutoFlow_${++counter}`;
          const nf = doc.createElementNS(NS.bpmn, 'bpmn:sequenceFlow');
          nf.setAttribute('id', newId);
          nf.setAttribute('sourceRef', src);
          nf.setAttribute('targetRef', tgt);
          processEl.appendChild(nf);

          // Optional DI if both shapes exist
          const srcShape = select(`.//bpmndi:BPMNShape[@bpmnElement="${src}"]/dc:Bounds`, plane)[0];
          const tgtShape = select(`.//bpmndi:BPMNShape[@bpmnElement="${tgt}"]/dc:Bounds`, plane)[0];
          if (srcShape && tgtShape) {
            const e = doc.createElementNS(NS.bpmndi, 'bpmndi:BPMNEdge');
            e.setAttribute('id', `${newId}_di`);
            e.setAttribute('bpmnElement', newId);

            const sx = parseFloat(srcShape.getAttribute('x')) + parseFloat(srcShape.getAttribute('width'));
            const sy = parseFloat(srcShape.getAttribute('y')) + parseFloat(srcShape.getAttribute('height')) / 2;
            const tx = parseFloat(tgtShape.getAttribute('x'));
            const ty = parseFloat(tgtShape.getAttribute('y')) + parseFloat(tgtShape.getAttribute('height')) / 2;

            const w1 = doc.createElementNS(NS.di, 'di:waypoint'); w1.setAttribute('x', String(sx)); w1.setAttribute('y', String(sy));
            const w2 = doc.createElementNS(NS.di, 'di:waypoint'); w2.setAttribute('x', String(tx)); w2.setAttribute('y', String(ty));
            e.appendChild(w1); e.appendChild(w2);
            plane.appendChild(e);
          }
        });
      });
    });

    // ---- THEN proceed to remove touching flows and masked nodes (your existing safe code) ----
    // (keep your removeAllAssociationsTouchingId, removeDIEdgeForElement, removeDIShapeForElement, etc.)



    // Remove all flows touching the masked task (safely, once)
    const toRemove = incoming.concat(outgoing);
    toRemove.forEach((f) => {
      if (!f) return;
      const fid = f.getAttribute('id');
      if (!fid || removedFlowIds.has(fid)) return;

      // 2a) remove any associations linked to this flow (e.g., coupling annotations)
      removeAllAssociationsTouchingId(doc, defs, plane, processEl, fid);

      // 2b) remove the flow's DI + element
      removeDIEdgeForElement(plane, fid);
      if (f.parentNode) f.parentNode.removeChild(f);
      removedFlowIds.add(fid);
    });

    // 3) Remove the masked task's own associations + any orphaned annotations
    removeAllAssociationsTouchingId(doc, defs, plane, processEl, mid);

    // 4) Remove the masked task node + its DI
    const node = byId.get(mid);
    if (node && node.parentNode) node.parentNode.removeChild(node);
    removeDIShapeForElement(plane, mid);
  });

  return maskedIds.length;
}

(function main() {
  const { input, output, mode, threshold, privacy, privacyDir, includeSingletons, clearOld } = parseArgs();
  const xml = fs.readFileSync(input, 'utf8');
  const doc = new DOMParser().parseFromString(xml, 'text/xml');

  const defs = select('/bpmn:definitions', doc)[0];
  const processEl = select('//bpmn:process[1]', doc)[0];
  if (!processEl) throw new Error('No bpmn:process found');

  if (clearOld) clearOldFragments(doc, processEl, defs);

  let count = 0;
  if (mode === 'mask') {
    count = maskByPrivacy(doc, defs, processEl, privacy, privacyDir);
    console.log(`Masked ${count} task(s)`);
  } else {
    count = fragmentByCoupling(doc, defs, processEl, threshold, includeSingletons);
    console.log(`Fragmented into ${count} group(s)`);
  }

  fs.writeFileSync(output, new XMLSerializer().serializeToString(doc), 'utf8');
  console.log(`Wrote ${output}`);
})();
