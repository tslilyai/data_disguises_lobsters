class RegisterNotification < ApplicationMailer
  def notify(user, privkey)
    @privkey = privkey 

    mail(
      :to => user.email,
      :subject => "[#{Rails.application.name}] Your account has been registered."
    )
  end
end
