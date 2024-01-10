import praw, time, datetime, psaw

TIME_TO_WAIT = 2 #time to wait between requests to server, so as not to increase load too much

def get_elapsed(current: float, previous: float) -> str:
    '''
    Return the elapsed time as a pretty string for reporting.
    Arguments:
        `current: float`: the current (later) time in seconds.
        `previous: float`: the previous (earlier) time in seconds.
    Returns:
        `str`: a string for the format f'{hours} hours, {minutes} minutes, and {seconds} seconds', 
            with hours and minutes being integers, and seconds rounded to three decimal points.
    '''
    hours = int(current-previous) // 3600
    min_begin = previous + hours * 3600
    minutes = int(current-min_begin) // 60
    sec_begin = previous + hours * 3600 + minutes * 60
    seconds = current - sec_begin
    return f'{hours} hours, {minutes} minutes, and {seconds:.3f} seconds'

def retrieve(reddit: praw.Reddit, subreddit: str, latest_post: float, elapsed_time: float, wait_time: float=TIME_TO_WAIT) -> list[praw.models.Submission]:
    '''
    Retrieve all posts on a subreddit in a time frame.
    Arguments:
        `reddit: praw.Reddit`: the authorized Reddit instance.
        `subreddit: str`: the name of the subreddit to request posts from.
        `latest_post: float`: the earliest time a post should be requested from, in seconds from Epoch.
        `elapsed_time: float`: how much time should be checked back from  `latest_post`, in seconds.
        Optional:
            `wait_time: float=TIME_TO_WAIT`: the amount of time to wait between iterations, to keep load on servers minimal.
    Returns:
        `list[praw.Submission]`: a list of all submissions made during the time period, sorted first posted to last posted.
    '''
    ps = psaw.PushshiftAPI(reddit)
    begin_time, end_time = latest_post, latest_post - elapsed_time
    current, posts, i = begin_time, [], 1
    while int(current) > int(end_time):
        current_post = None
        iter_start = time.time()
        
        #actually perform query, add to results
        temp_results = ps.search_submission(before=current, subreddit=subreddit, limit=None)
        temp_results = list(temp_results) #this is the part that actually requests the submissions from servers
        posts += temp_results
        
        #deal with time stuff
        previous, current = current, temp_results[-1].created_utc+1
        elapsed = get_elapsed(previous, current)
        remaining = get_elapsed(current, end_time) if current > end_time else '0 seconds'
        
        #report
        iter_time = time.time() - iter_start
        iter_elapsed = get_elapsed(iter_time, iter_start)
        time_rate = (iter_time) / (previous - current)
        remaining_time = max((current - end_time) * time_rate, 0)
        remaining_iter = max(remaining_time / iter_time, 0)
        remaining_elapsed = get_elapsed(remaining_time, 0)
        print(f'Retrieved {(i)*100:,}th post, spanning {elapsed} of real time.')
        print(f'\tFinished iteration in {iter_time:.3f} seconds.')
        print(f'\t{remaining} of real time remain.')
        print(f'\tAt this rate, expecting to finish {remaining_iter:.0f} iterations in {remaining_elapsed}.')
        
        #cleanup
        i += 1
        current_post = posts[-1]
        if int(current) > int(end_time):
            print(f'\tWaiting {wait_time} seconds before next iteration.')
            time.sleep(wait_time) #don't want to work those servers too hard
    return [post for post in posts if post.created_utc > end_time][::-1]