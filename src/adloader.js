import { LOAD_EXTERNAL_SCRIPT } from './activities/activities.js';
import { activityParams } from './activities/activityParams.js';
import { isActivityAllowed } from './activities/rules.js';

import { insertElement, logError, logWarn, setScriptAttributes } from './utils.js';

const _requestCache = new WeakMap();
// The below list contains modules or vendors whom Prebid allows to load external JS.
const _approvedLoadExternalJSList = [
  // Prebid maintained modules:
  'debugging',
  'outstream',
  // RTD modules:
  'aaxBlockmeter',
  'adagio',
  'adloox',
  'arcspan',
  'airgrid',
  'browsi',
  'brandmetrics',
  'clean.io',
  'humansecurityMalvDefense',
  'humansecurity',
  'confiant',
  'contxtful',
  'hadron',
  'mediafilter',
  'medianet',
  'azerionedge',
  'a1Media',
  'geoedge',
  'qortex',
  'dynamicAdBoost',
  '51Degrees',
  'symitridap',
  'wurfl',
  'nodalsAi',
  'anonymised',
  'optable',
  // UserId Submodules
  'justtag',
  'tncId',
  'ftrackId',
  'id5',
];

/**
 * Loads external javascript. Can only be used if external JS is approved by Prebid. See https://github.com/prebid/prebid-js-external-js-template#policy
 * Each unique URL will be loaded at most 1 time.
 * @param {string} url the url to load
 * @param {string} moduleType moduleType of the module requesting this resource
 * @param {string} moduleCode bidderCode or module code of the module requesting this resource
 * @param {function} [callback] callback function to be called after the script is loaded
 * @param {Document} [doc] the context document, in which the script will be loaded, defaults to loaded document
 * @param {object} attributes an object of attributes to be added to the script with setAttribute by [key] and [value]; Only the attributes passed in the first request of a url will be added.
 */
export function loadExternalScript(url, moduleType, moduleCode, callback, doc, attributes) {
  if (!isActivityAllowed(LOAD_EXTERNAL_SCRIPT, activityParams(moduleType, moduleCode))) {
    return;
  }

  if (!moduleCode || !url) {
    logError('cannot load external script without url and moduleCode');
    return;
  }
  if (!_approvedLoadExternalJSList.includes(moduleCode)) {
    logError(`${moduleCode} not whitelisted for loading external JavaScript`);
    return;
  }
  if (!doc) {
    doc = document; // provide a "valid" key for the WeakMap
  }
  // only load each asset once
  const storedCachedObject = getCacheObject(doc, url);
  if (storedCachedObject) {
    if (callback && typeof callback === 'function') {
      if (storedCachedObject.loaded) {
        // invokeCallbacks immediately
        callback();
      } else {
        // queue the callback
        storedCachedObject.callbacks.push(callback);
      }
    }
    return storedCachedObject.tag;
  }
  const cachedDocObj = _requestCache.get(doc) || {};
  const cacheObject = {
    loaded: false,
    tag: null,
    callbacks: []
  };
  cachedDocObj[url] = cacheObject;
  _requestCache.set(doc, cachedDocObj);

  if (callback && typeof callback === 'function') {
    cacheObject.callbacks.push(callback);
  }

  logWarn(`module ${moduleCode} is loading external JavaScript`);
  return requestResource(url, function () {
    cacheObject.loaded = true;
    try {
      for (let i = 0; i < cacheObject.callbacks.length; i++) {
        cacheObject.callbacks[i]();
      }
    } catch (e) {
      logError('Error executing callback', 'adloader.js:loadExternalScript', e);
    }
  }, doc, attributes);

  function requestResource(tagSrc, callback, doc, attributes) {
    if (!doc) {
      doc = document;
    }
    var jptScript = doc.createElement('script');
    jptScript.type = 'text/javascript';
    jptScript.async = true;

    const cacheObject = getCacheObject(doc, url);
    if (cacheObject) {
      cacheObject.tag = jptScript;
    }

    if (jptScript.readyState) {
      jptScript.onreadystatechange = function () {
        if (jptScript.readyState === 'loaded' || jptScript.readyState === 'complete') {
          jptScript.onreadystatechange = null;
          callback();
        }
      };
    } else {
      jptScript.onload = function () {
        callback();
      };
    }

    jptScript.src = tagSrc;

    if (attributes) {
      setScriptAttributes(jptScript, attributes);
    }

    // add the new script tag to the page
    insertElement(jptScript, doc);

    return jptScript;
  }
  function getCacheObject(doc, url) {
    const cachedDocObj = _requestCache.get(doc);
    if (cachedDocObj && cachedDocObj[url]) {
      return cachedDocObj[url];
    }
    return null; // return new cache object?
  }
};
