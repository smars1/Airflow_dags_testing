import smtplib

try:
    server = smtplib.SMTP('email-smtp.us-east-1.amazonaws.com', 587)
    #server = smtplib.SMTP('vpce-0055e17f04e3a98e6-9jh9zurh.email-smtp.us-east-1.vpce.amazonaws.com', 587)
    server.starttls()
    server.login('AKIA2E5SJ2OSQA2MD24G', 'BBRukApeGvaXiCoYYWu4nStNh0GI5y8lY9jX9TlHxt2e')
    tolist = ['diego.pineda@factorit.com']
    to_emails = ', '.join(tolist) 
    message = f"From: asaravia@entel.cl\nTo: {to_emails}\nSubject: Prueba SMTP-ENTEL-HOST\n\nEste es un correo de prueba."
    server.sendmail(from_addr='asaravia@entel.cl',to_addrs=tolist, msg=message)
    print("Correo enviado exitosamente, se logro xd")
except Exception as e:
    print("Error al enviar el correo:", e)
finally:
    server.quit()