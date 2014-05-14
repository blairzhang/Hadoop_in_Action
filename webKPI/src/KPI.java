import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class KPI {
	private String remote_addr; //ip addr
	private String remote_user;	//user name
	private String time_local;
	private String request;
	private String status;
	private String body_bytes_sent;
	private String http_referer;
	private String http_user_agent;
	private boolean valid = true;
	
	public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public Date getTime_local_Date() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
        return df.parse(this.time_local);
    }
    
    public String getTime_local_Date_hour() throws ParseException{
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
        return df.format(this.getTime_local_Date());
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }
    
    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }
	public void parser(String line){
		String[] arr = line.split(" ");
		
		if(arr.length > 11){
			this.setRemote_addr(arr[0]);
			this.setRemote_user(arr[1]);
			this.setTime_local(arr[3]);
			this.setRequest(arr[6]);
			this.setStatus(arr[8]);
			this.setBody_bytes_sent(arr[9]);
			this.setHttp_referer(arr[10]);
			this.setHttp_user_agent(arr[11]);
			this.setValid(true);
			
			if(Integer.parseInt(this.getStatus()) >= 400){
				this.setValid(false);
			}
		}
		else
		{
			this.setValid(false);
		}

	}
}