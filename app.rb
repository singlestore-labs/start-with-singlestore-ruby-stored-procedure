require 'mysql2'

def main
  begin

    # TODO: pull from config
    client = Mysql2::Client.new(:host => "127.0.0.1", :username => "root", :password => "", :database => "acme")

    id = create(client, 'Inserted row')
    puts "Inserted row #{id}"

    row = read_one(client, id);
    if (row)
      puts "Read row: #{row}"
    else
      puts "Read row: missing"
    end

    update(client, id, "Updated row")
    puts "Updated row id #{id}"
    
    rows = read_all(client)
    puts "Read all rows:"
    rows.each do |row|
      puts "  #{row}"
    end

    delete(client, id)
    puts "Deleted row id #{id}"
  
  rescue => exception
    puts $!
    puts exception.backtrace
    raise
  end

end

def create(client, content)
  stmt = client.prepare("CALL messages_create(?)")
  result = stmt.execute(content)
  id = result.first['id']
  # Drain the result sets
  while client.next_result
    client.store_result
  end
  stmt.close
  id
end

def read_one(client, id)
  stmt = client.prepare("CALL messages_read_by_id(?)")
  result = stmt.execute(id)
  message = result.first
  stmt.close
  message
end

def read_all(client)
  result = client.query("CALL messages_read_all()")
  # Drain the result sets
  while client.next_result
    client.store_result
  end
  result
end

def update(client, id, content)
  stmt = client.prepare("CALL messages_update(?,?)")
  stmt.execute(id, content)
  stmt.close
end

def delete(client, id)
  stmt = client.prepare("CALL messages_delete(?)")
  stmt.execute(id)
  stmt.close
end

if __FILE__ == $0
  main
end
