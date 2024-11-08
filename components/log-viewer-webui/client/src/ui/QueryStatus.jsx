import {
    useEffect,
    useRef,
    useState,
} from "react";

import {AxiosError} from "axios";

import {
    submitExtractIrJob,
    submitExtractJsonJob,
} from "../api/query.js";
import {QUERY_LOADING_STATES} from "../typings/query.js";
import Loading from "./Loading.jsx";


/**
 * Submits queries and renders the query states.
 *
 * @return {React.ReactElement}
 */
const QueryStatus = () => {
    const [queryState, setQueryState] = useState(QUERY_LOADING_STATES.SUBMITTING);
    const [errorMsg, setErrorMsg] = useState(null);
    const isFirstRun = useRef(true);

    useEffect(() => {
        if (false === isFirstRun.current) {
            return;
        }
        isFirstRun.current = false;

        const searchParams = new URLSearchParams(window.location.search);
        const origFileId = searchParams.get("origFileId");
        const logEventIdx = searchParams.get("logEventIdx");
        const archiveId = searchParams.get("archiveId");
        const timestamp = searchParams.get("timestamp");
        if (null !== origFileId && null !== logEventIdx) {
            submitExtractIrJob(
                origFileId,
                Number(logEventIdx),
                () => {
                    setQueryState(QUERY_LOADING_STATES.WAITING);
                }
            )
                .then(({data}) => {
                    setQueryState(QUERY_LOADING_STATES.LOADING);

                    const innerLogEventNum = logEventIdx - data.begin_msg_ix + 1;
                    window.location = `/log-viewer/index.html?filePath=/ir/${data.path}` +
                        `#logEventNum=${innerLogEventNum}`;
                })
                .catch((e) => {
                    let msg = "Unknown error.";
                    if (e instanceof AxiosError) {
                        msg = e.message;
                        if ("undefined" !== typeof e.response) {
                            if ("undefined" !== typeof e.response.data.message) {
                                msg = e.response.data.message;
                            } else {
                                msg = e.response.statusText;
                            }
                        }
                    }
                    console.error(msg, e);
                    setErrorMsg(msg);
                });
        } else if (null !== archiveId && null !== timestamp) {
            submitExtractJsonJob(
                archiveId,
                Number(timestamp),
                () => {
                    setQueryState(QUERY_LOADING_STATES.WAITING);
                }
            )
                .then(({data}) => {
                    setQueryState(QUERY_LOADING_STATES.LOADING);

                    const innerLogEventNum = 1;
                    window.location = `/log-viewer/index.html?filePath=/ir/${data.path}` +
                        `#logEventNum=${innerLogEventNum}`;
                })
                .catch((e) => {
                    let msg = "Unknown error.";
                    if (e instanceof AxiosError) {
                        msg = e.message;
                        if ("undefined" !== typeof e.response) {
                            if ("undefined" !== typeof e.response.data.message) {
                                msg = e.response.data.message;
                            } else {
                                msg = e.response.statusText;
                            }
                        }
                    }
                    console.error(msg, e);
                    setErrorMsg(msg);
                });
        } else {
            const error = "Queries parameters are missing from the URL " +
             "parameters.";

            console.error(error);
            setErrorMsg(error);
        }
    }, []);

    return (
        <Loading
            currentState={queryState}
            errorMsg={errorMsg}/>
    );
};

export default QueryStatus;
