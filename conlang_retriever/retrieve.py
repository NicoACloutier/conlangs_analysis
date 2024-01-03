import praw, time, typing

TIME_TO_WAIT = 2 #time to wait between requests to server, so as not to increase load too much

def make_instance(client_id: str, client_secret: str, username: str) -> praw.Reddit:
    '''
    Make a Reddit instance for data retrieval.
    Arguments:
        `client_id: str`: the client ID for this instance.
        `client_secret: str`: the secret key for this instance.
        `username: str`: the name of the user agent for retrieval.
    Returns:
        `praw.Reddit`: a `praw` read-only Reddit instance.
    '''
    return praw.Reddit(client_id, client_secret, username)

def get_elapsed(current: float, previous: float) -> str:
    '''
    Return the elapsed time as a pretty string for reporting.
    '''
    hours = int(current-previous) // 3600
    min_begin = previous + hours * 3600
    minutes = int(current-min_begin) // 60
    sec_begin = previous + hours * 3600 + minutes * 60
    seconds = current - sec_begin
    return f'{hours} hours, {minutes} minutes, and {seconds:.3f} seconds'

def retrieve(reddit: praw.Reddit, subreddit: str, begin_time: float, end_time: float) -> list[praw.Submission]:
    '''
    Retrieve all posts on a subreddit in a time frame.
    Arguments:
        `reddit: praw.Reddit`: the authorized Reddit instance.
        `subreddit: str`: the name of the subreddit to request posts from.
        `begin_time: str`: the first time to request submissions from.
        `end_time: str`: the last time to request submissions from.
    Returns:
        `list[praw.Submission]`: a list of all submissions made during the time period, sorted first posted to last posted.
    '''
    current, posts, i = begin_time, [], 0
    while current < end_time:
        iter_start = time.time()
        
        #actually perform query, add to results
        query = f'timestamp:{current}..{end_time}'
        temp_results = reddit.subreddit(subreddit).search(query, sort='new')
        temp_results = list(temp_results)
        posts += temp_results
        
        #deal with time stuff
        previous, current = current, temp_results[-1].created_utc
        elapsed = get_elapsed(current, previous)
        remaining = get_elapsed(end_time, current)
        
        #report
        iter_time = time.time() - iter_start
        iter_elapsed = get_elapsed(iter_time, iter_start)
        time_rate = (iter_time) / (current - previous)
        remaining_time = (end_time - current) * time_rate
        remaining_iter = remaining_time / iter_time
        remaining_elapsed = get_elapsed(remaining_time, 0)
        print(f'Retrieved {i:,},000th post, spanning {elapsed} of real time. Finished iteration in {iter_time:.2f}.')
        print(f'\t{remaining} of real time remain. At this rate, expecting to finish {remaining_iter:.0f} iterations in {remaining_elapsed}.')
        print(f'\tWaiting {TIME_TO_WAIT} seconds before next iteration.')
        time.sleep(TIME_TO_WAIT) #don't want to work those servers too hard
    return [post for post in posts if post.created_utc < end_time][::-1]