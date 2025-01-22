import {MODULE_TYPE_RTD} from '../src/activities/modules.js';
import {loadExternalScript} from '../src/adloader.js';
import {submodule} from '../src/hook.js';
import {
  deepAccess,
  // deepSetValue,
  mergeDeep,
  prefixLog,
} from '../src/utils.js';

const MODULE_NAME = 'optable';
export const LOG_PREFIX = `[${MODULE_NAME} RTD]:`;
const {logMessage, logWarn, logError} = prefixLog(LOG_PREFIX);

/**
 * Extracts the parameters for Optable RTD module from the config object passed at instantiation
 * @param {Object} moduleConfig Configuration object for the module
 * @param {Object} reqBidsConfigObj Configuration object for bid request
 */
export const extractConfig = (moduleConfig, reqBidsConfigObj) => {
  let bundleUrl = deepAccess(moduleConfig, 'params.bundleUrl', null);
  let propagateTargeting = deepAccess(moduleConfig, 'params.propagateTargeting', false);

  // If present, trim the bundle URL
  if (typeof bundleUrl === 'string') {
    bundleUrl = bundleUrl.trim();
  }

  // Verify that bundleUrl is a valid URL: either a full URL, relative
  // path (/path/to/file.js), or a protocol-relative URL (//example.com/path/to/file.js)
  if (typeof bundleUrl === 'string' && bundleUrl.length && !(
    bundleUrl.startsWith('http://') ||
    bundleUrl.startsWith('https://') ||
    bundleUrl.startsWith('/'))
  ) {
    throw new Error(LOG_PREFIX + ' Invalid URL format for bundleUrl in moduleConfig');
  }

  return {bundleUrl, propagateTargeting};
}

/**
 * @param {Object} optableBundle Optable JS bundle
 * @param {Object} reqBidsConfigObj Bid request configuration object
 */
export const mergeOptableData = (optableBundle, reqBidsConfigObj) => {
  // Get UID2 data from Optable and merge it into the global ORTB2 object
  mergeDeep(
    reqBidsConfigObj.ortb2Fragments.global.user.ext,
    optableBundle.prebidORTB2FromCache(),
  );
};

/**
 * @param {Object} reqBidsConfigObj Bid request configuration object
 * @param {Function} callback Called on completion
 * @param {Object} moduleConfig Configuration for Optable RTD module
 * @param {Object} userConsent
 */
export const getBidRequestData = (reqBidsConfigObj, callback, moduleConfig, userConsent) => {
  try {
    // Get configuration parameters
    const {bundleUrl, propagateTargeting} = extractConfig(moduleConfig, reqBidsConfigObj);
    logMessage('Optable JS bundle URL ', bundleUrl);
    logMessage('Propagate targeting: ', propagateTargeting);

    if (bundleUrl) {
      // If bundleUrl is present, load the Optable JS bundle
      // by using the loadExternalScript function
      logWarn('Custom bundle URL found in config: ', bundleUrl);

      // Load Optable JS bundle and merge the data
      loadExternalScript(bundleUrl, MODULE_TYPE_RTD, MODULE_NAME, () => {
        const optable = /** @type {Object} */ (window.optable);
        logMessage('Successfully loaded Optable JS bundle');

        logMessage('optable: ', optable);
        logMessage('reqBidsConfigObj: ', reqBidsConfigObj);

        mergeOptableData(optable, reqBidsConfigObj);
        callback();
      }, document);
    } else {
      // At this point, we assume that the Optable JS bundle is already
      // present on the page. If it is, we can directly merge the data
      // by passing the callback to the optable.cmd.push function.
      logMessage('Custom bundle URL not found in config');
      window.optable = window.optable || { cmd: [] };
      window.optable.cmd.push(function() {
        logMessage('Optable JS bundle found on the page');
        logMessage('optable: ', window.optable);
        logMessage('reqBidsConfigObj: ', reqBidsConfigObj);
        mergeOptableData(window.optable, reqBidsConfigObj);
        callback();
      });
    }

    if (propagateTargeting) {
      // Propagate targeting data to GAM
    }
  } catch (error) {
    // If an error occurs, log it and call the callback
    // to continue with the auction
    logError(error);
    callback();
  }
}

export const getTargetingData = (adUnits, config, userConsent) => {
  logMessage('getTargetingData called with adUnits: ', adUnits);
  return {};
};

/**
 * Dummy init function
 * @param {Object} config Module configuration
 * @param {boolean} userConsent User consent
 * @returns true
 */
const init = (config, userConsent) => {
  return true;
}

// Optable RTD submodule
export const optableSubmodule = {
  name: MODULE_NAME,
  init,
  getBidRequestData,
  getTargetingData,
}

// Register the Optable RTD submodule
submodule('realTimeData', optableSubmodule);
